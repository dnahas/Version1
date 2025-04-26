#region imports
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using System.Drawing;
using QuantConnect;
using QuantConnect.Algorithm.Framework;
using QuantConnect.Algorithm.Framework.Selection;
using QuantConnect.Algorithm.Framework.Alphas;
using QuantConnect.Algorithm.Framework.Portfolio;
using QuantConnect.Algorithm.Framework.Portfolio.SignalExports;
using QuantConnect.Algorithm.Framework.Execution;
using QuantConnect.Algorithm.Framework.Risk;
using QuantConnect.Algorithm.Selection;
using QuantConnect.Api;
using QuantConnect.Parameters;
using QuantConnect.Benchmarks;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Util;
using QuantConnect.Interfaces;
using QuantConnect.Algorithm;
using QuantConnect.Indicators;
using QuantConnect.Data;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Data.Consolidators;
using QuantConnect.Data.Custom;
using QuantConnect.Data.Custom.IconicTypes;
using QuantConnect.DataSource;
using QuantConnect.Data.Fundamental;
using QuantConnect.Data.Market;
using QuantConnect.Data.Shortable;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Notifications;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Orders.Fills;
using QuantConnect.Orders.OptionExercise;
using QuantConnect.Orders.Slippage;
using QuantConnect.Orders.TimeInForces;
using QuantConnect.Python;
using QuantConnect.Scheduling;
using QuantConnect.Securities;
using QuantConnect.Securities.Equity;
using QuantConnect.Securities.Future;
using QuantConnect.Securities.Option;
using QuantConnect.Securities.Positions;
using QuantConnect.Securities.Forex;
using QuantConnect.Securities.Crypto;
using QuantConnect.Securities.CryptoFuture;
using QuantConnect.Securities.Interfaces;
using QuantConnect.Securities.Volatility;
using QuantConnect.Storage;
using QuantConnect.Statistics;
using QCAlgorithmFramework = QuantConnect.Algorithm.QCAlgorithm;
using QCAlgorithmFrameworkBridge = QuantConnect.Algorithm.QCAlgorithm;
using QuantConnect.Algorithm.Framework.Alphas.Analysis;
using Accord;
using QLNet;
using Accord.Math;
#endregion

namespace QuantConnect {
    public class ProtectivePutModel : RiskManagementModel {
        // core parameters
        private readonly decimal _putStrikePercent = 0.90m;    // deeper otm puts
        private readonly decimal _hedgeRatio = 0.05m;          // only hedge 5% of position
        private readonly decimal _drawdownThreshold = -0.12m;   // only start hedging at 12% drawdown
        
        // option selection parameters
        private readonly int _minDaysToExpiration = 60;
        private readonly int _maxDaysToExpiration = 90;        // longer dated options
        private readonly decimal _minOptionVolume = 500;       // strict liquidity requirement
        private readonly decimal _maxBidAskSpread = 0.05m;     // max 5% spread
        
        // internal tracking
        private Dictionary<Symbol, Symbol> _optionSymbols;
        private HashSet<Symbol> _optionUniverses;
        private decimal _portfolioHigh = 0m;
        private DateTime _lastUpdate = DateTime.MinValue;
        private readonly TimeSpan _minimumTimeSpan = TimeSpan.FromDays(1);  // prevent overtrading

        public ProtectivePutModel() {
            _optionSymbols = new Dictionary<Symbol, Symbol>();
            _optionUniverses = new HashSet<Symbol>();
        }
        
        public override IEnumerable<IPortfolioTarget> ManageRisk(
            QCAlgorithm algorithm, 
            IPortfolioTarget[] targets) {
            
            var riskAdjustedTargets = new List<IPortfolioTarget>();
            
            // portfolio high water mark
            _portfolioHigh = Math.Max(_portfolioHigh, algorithm.Portfolio.TotalPortfolioValue);
            var drawdown = algorithm.Portfolio.TotalPortfolioValue / _portfolioHigh - 1;
            
            // only check once per day to reduce turnover
            if (algorithm.Time - _lastUpdate < _minimumTimeSpan) {
                return riskAdjustedTargets;
            }
            _lastUpdate = algorithm.Time;
            
            // if drawdown is not severe enough, clear all hedges
            var needHedge = drawdown <= _drawdownThreshold;
            if (!needHedge) {
                foreach (var optionSymbol in _optionSymbols.Values) {
                    if (algorithm.Securities.ContainsKey(optionSymbol) && 
                        algorithm.Portfolio[optionSymbol].Invested) {
                        riskAdjustedTargets.Add(new PortfolioTarget(optionSymbol, 0));
                    }
                }
                _optionSymbols.Clear();
                return riskAdjustedTargets;
            }
            
            // process each equity position
            foreach(var kvp in algorithm.Portfolio) {
                var equity = kvp.Value;
                if(!equity.Invested || equity.Symbol.SecurityType != SecurityType.Equity) continue;
                
                var quantity = Math.Abs(equity.Quantity);
                if(quantity == 0) continue;

                // ensure option universe is set up
                if(!_optionUniverses.Contains(equity.Symbol)) {
                    algorithm.AddOption(equity.Symbol);
                    _optionUniverses.Add(equity.Symbol);
                    continue;
                }

                // get option chain
                var chains = algorithm.OptionChainProvider.GetOptionContractList(equity.Symbol, algorithm.Time);
                if(chains == null || !chains.Any()) continue;
                
                var currentPrice = equity.Price;
                var targetStrike = currentPrice * _putStrikePercent;
                var targetExpiry = algorithm.Time.AddDays(_minDaysToExpiration);
                
                // filter puts
                var puts = chains
                    .Where(x => x.ID.OptionRight == OptionRight.Put &&
                              x.ID.Date >= algorithm.Time.AddDays(_minDaysToExpiration) &&
                              x.ID.Date <= algorithm.Time.AddDays(_maxDaysToExpiration) &&
                              x.ID.StrikePrice >= targetStrike * 0.95m &&
                              x.ID.StrikePrice <= targetStrike * 1.05m &&
                              algorithm.Securities.ContainsKey(x))
                    .OrderBy(x => Math.Abs((x.ID.Date - targetExpiry).TotalDays))
                    .ThenBy(x => Math.Abs(x.ID.StrikePrice - targetStrike))
                    .ToList();
                
                if(!puts.Any()) continue;
                
                // filter for liquidity
                var liquidPuts = puts.Where(x => {
                    var option = algorithm.Securities[x];
                    var bidPrice = option.BidPrice;
                    var askPrice = option.AskPrice;
                    var volume = option.Volume;
                    
                    if (volume < _minOptionVolume) return false;
                    if (askPrice <= 0) return false;
                    
                    var spread = (askPrice - bidPrice) / askPrice;
                    return spread <= _maxBidAskSpread;
                }).ToList();
                
                if(!liquidPuts.Any()) continue;
                var selectedPut = liquidPuts.First();
                
                // manage existing positions
                if(_optionSymbols.TryGetValue(equity.Symbol, out var existingOption)) {
                    if(!algorithm.Securities.ContainsKey(existingOption)) continue;
                    
                    var existingPosition = algorithm.Portfolio[existingOption];
                    if(existingPosition.Invested && 
                       existingPosition.Quantity == quantity * _hedgeRatio) continue;
                    
                    riskAdjustedTargets.Add(new PortfolioTarget(existingOption, 0));
                }
                
                // scale hedge ratio based on drawdown severity
                var hedgeScale = Math.Min(1.0m, Math.Abs(drawdown) / 0.2m);  // scale up to 20% drawdown
                var putQuantity = (int)(quantity * _hedgeRatio * hedgeScale);
                
                if(putQuantity > 0) {
                    _optionSymbols[equity.Symbol] = selectedPut;
                    riskAdjustedTargets.Add(new PortfolioTarget(selectedPut, putQuantity));
                    algorithm.Debug($"Protective put added for {equity.Symbol}: {putQuantity} contracts of {selectedPut}, " + 
                                  $"Strike: {selectedPut.ID.StrikePrice}, Expiry: {selectedPut.ID.Date}, " +
                                  $"Drawdown: {drawdown:P2}, Volume: {algorithm.Securities[selectedPut].Volume}");
                }
            }
            
            return riskAdjustedTargets;
        }
    }
}