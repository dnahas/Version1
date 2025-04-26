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
            
            // Add initialization log
            Console.WriteLine("ProtectivePutModel: Initialized with parameters: " +
                $"PutStrikePercent={_putStrikePercent}, " +
                $"HedgeRatio={_hedgeRatio}, " +
                $"DrawdownThreshold={_drawdownThreshold}, " +
                $"MinDaysToExpiration={_minDaysToExpiration}, " +
                $"MaxDaysToExpiration={_maxDaysToExpiration}, " +
                $"MinOptionVolume={_minOptionVolume}, " +
                $"MaxBidAskSpread={_maxBidAskSpread}");
        }
        
        public override IEnumerable<IPortfolioTarget> ManageRisk(
            QCAlgorithm algorithm, 
            IPortfolioTarget[] targets) {
            
            var currentTime = algorithm.Time.ToString("yyyy-MM-dd HH:mm:ss");
            Console.WriteLine($"ProtectivePutModel: ManageRisk called at {currentTime}");
            
            var riskAdjustedTargets = new List<IPortfolioTarget>();
            
            // portfolio high water mark
            var oldPortfolioHigh = _portfolioHigh;
            _portfolioHigh = Math.Max(_portfolioHigh, algorithm.Portfolio.TotalPortfolioValue);
            var drawdown = algorithm.Portfolio.TotalPortfolioValue / _portfolioHigh - 1;
            
            Console.WriteLine($"ProtectivePutModel: Portfolio value: {algorithm.Portfolio.TotalPortfolioValue:C}, " +
                $"High water mark: {_portfolioHigh:C}, " +
                $"Current drawdown: {drawdown:P2}");
            
            if (oldPortfolioHigh != _portfolioHigh) {
                Console.WriteLine($"ProtectivePutModel: New high water mark set: {_portfolioHigh:C}");
            }
            
            // only check once per day to reduce turnover
            if (algorithm.Time - _lastUpdate < _minimumTimeSpan) {
                Console.WriteLine($"ProtectivePutModel: Skipping update, last update was at {_lastUpdate}");
                return riskAdjustedTargets;
            }
            _lastUpdate = algorithm.Time;
            Console.WriteLine($"ProtectivePutModel: Update time set to {_lastUpdate}");
            
            // if drawdown is not severe enough, clear all hedges
            var needHedge = drawdown <= _drawdownThreshold;
            Console.WriteLine($"ProtectivePutModel: Need hedge? {needHedge} (Threshold: {_drawdownThreshold:P2})");
            
            if (!needHedge) {
                if (_optionSymbols.Count > 0) {
                    Console.WriteLine($"ProtectivePutModel: Drawdown not severe enough, clearing {_optionSymbols.Count} hedges");
                    foreach (var optionSymbol in _optionSymbols.Values) {
                        if (algorithm.Securities.ContainsKey(optionSymbol) && 
                            algorithm.Portfolio[optionSymbol].Invested) {
                            Console.WriteLine($"ProtectivePutModel: Removing hedge for {optionSymbol}");
                            riskAdjustedTargets.Add(new PortfolioTarget(optionSymbol, 0));
                        }
                    }
                    _optionSymbols.Clear();
                } else {
                    Console.WriteLine("ProtectivePutModel: No existing hedges to clear");
                }
                return riskAdjustedTargets;
            }
            
            Console.WriteLine($"ProtectivePutModel: Processing {algorithm.Portfolio.Count} portfolio positions");
            int processedCount = 0;
            
            // process each equity position
            foreach(var kvp in algorithm.Portfolio) {
                var equity = kvp.Value;
                if(!equity.Invested || equity.Symbol.SecurityType != SecurityType.Equity) continue;
                
                var quantity = Math.Abs(equity.Quantity);
                if(quantity == 0) continue;

                processedCount++;
                Console.WriteLine($"ProtectivePutModel: Processing position {equity.Symbol}, " +
                    $"Quantity: {quantity}, Price: {equity.Price:C}");

                // ensure option universe is set up
                if(!_optionUniverses.Contains(equity.Symbol)) {
                    Console.WriteLine($"ProtectivePutModel: Adding option universe for {equity.Symbol}");
                    algorithm.AddOption(equity.Symbol);
                    _optionUniverses.Add(equity.Symbol);
                    continue;
                }

                // get option chain using the recommended method
                var optionChain = algorithm.OptionChain(equity.Symbol);
                if(optionChain == null || !optionChain.Any()) {
                    Console.WriteLine($"ProtectivePutModel: No option chains available for {equity.Symbol}");
                    continue;
                }
                
                Console.WriteLine($"ProtectivePutModel: Found {optionChain.Count()} option contracts for {equity.Symbol}");
                
                var currentPrice = equity.Price;
                var targetStrike = currentPrice * _putStrikePercent;
                var targetExpiry = algorithm.Time.AddDays(_minDaysToExpiration);
                
                Console.WriteLine($"ProtectivePutModel: Target strike: {targetStrike:C} " +
                    $"({_putStrikePercent:P0} of {currentPrice:C}), " +
                    $"Target expiry: {targetExpiry:yyyy-MM-dd}");
                
                // filter puts
                var puts = optionChain
                    .Where(x => x.Right == OptionRight.Put &&
                              x.Expiry >= algorithm.Time.AddDays(_minDaysToExpiration) &&
                              x.Expiry <= algorithm.Time.AddDays(_maxDaysToExpiration) &&
                              x.Strike >= targetStrike * 0.95m &&
                              x.Strike <= targetStrike * 1.05m &&
                              algorithm.Securities.ContainsKey(x.Symbol))
                    .OrderBy(x => Math.Abs((x.Expiry - targetExpiry).TotalDays))
                    .ThenBy(x => Math.Abs(x.Strike - targetStrike))
                    .ToList();
                
                Console.WriteLine($"ProtectivePutModel: Found {puts.Count} puts matching criteria " +
                    $"(DTE: {_minDaysToExpiration}-{_maxDaysToExpiration}, " +
                    $"Strike: {targetStrike * 0.95m:C}-{targetStrike * 1.05m:C})");
                
                if(!puts.Any()) continue;
                
                // filter for liquidity
                var liquidPuts = puts.Where(x => {
                    var option = algorithm.Securities[x.Symbol];
                    var bidPrice = option.BidPrice;
                    var askPrice = option.AskPrice;
                    var volume = option.Volume;
                    
                    if (volume < _minOptionVolume) return false;
                    if (askPrice <= 0) return false;
                    
                    var spread = (askPrice - bidPrice) / askPrice;
                    return spread <= _maxBidAskSpread;
                }).ToList();
                
                Console.WriteLine($"ProtectivePutModel: Found {liquidPuts.Count} liquid puts " +
                    $"(Min volume: {_minOptionVolume}, Max spread: {_maxBidAskSpread:P0})");
                
                if(!liquidPuts.Any()) continue;
                var selectedPut = liquidPuts.First();
                
                Console.WriteLine($"ProtectivePutModel: Selected put: {selectedPut.Symbol}, " +
                    $"Strike: {selectedPut.Strike:C}, " +
                    $"Expiry: {selectedPut.Expiry:yyyy-MM-dd}, " +
                    $"Volume: {algorithm.Securities[selectedPut.Symbol].Volume}, " +
                    $"Bid: {algorithm.Securities[selectedPut.Symbol].BidPrice:C}, " +
                    $"Ask: {algorithm.Securities[selectedPut.Symbol].AskPrice:C}");
                
                // manage existing positions
                if(_optionSymbols.TryGetValue(equity.Symbol, out var existingOption)) {
                    Console.WriteLine($"ProtectivePutModel: Found existing hedge for {equity.Symbol}: {existingOption}");
                    
                    if(!algorithm.Securities.ContainsKey(existingOption)) {
                        Console.WriteLine($"ProtectivePutModel: Existing option {existingOption} no longer available");
                        continue;
                    }
                    
                    var existingPosition = algorithm.Portfolio[existingOption];
                    if(existingPosition.Invested && 
                       existingPosition.Quantity == quantity * _hedgeRatio) {
                        Console.WriteLine($"ProtectivePutModel: Existing position for {equity.Symbol} is already optimal " +
                            $"(Quantity: {existingPosition.Quantity})");
                        continue;
                    }
                    
                    Console.WriteLine($"ProtectivePutModel: Closing existing position for {equity.Symbol}: {existingOption}");
                    riskAdjustedTargets.Add(new PortfolioTarget(existingOption, 0));
                }
                
                // scale hedge ratio based on drawdown severity
                var hedgeScale = Math.Min(1.0m, Math.Abs(drawdown) / 0.2m);  // scale up to 20% drawdown
                var putQuantity = (int)(quantity * _hedgeRatio * hedgeScale);
                
                Console.WriteLine($"ProtectivePutModel: Hedge scale: {hedgeScale:F2} " +
                    $"(based on drawdown {drawdown:P2}), " +
                    $"Target put quantity: {putQuantity} " +
                    $"({_hedgeRatio:P0} of position {quantity} × scale {hedgeScale:F2})");
                
                if(putQuantity > 0) {
                    _optionSymbols[equity.Symbol] = selectedPut.Symbol;
                    riskAdjustedTargets.Add(new PortfolioTarget(selectedPut.Symbol, putQuantity));
                    Console.WriteLine($"ProtectivePutModel: Added protective put for {equity.Symbol}: " +
                                    $"{putQuantity} contracts of {selectedPut.Symbol}, " + 
                                    $"Strike: {selectedPut.Strike:C}, " +
                                    $"Expiry: {selectedPut.Expiry:yyyy-MM-dd}, " +
                                    $"Drawdown: {drawdown:P2}, " +
                                    $"Volume: {algorithm.Securities[selectedPut.Symbol].Volume}");
                } else {
                    Console.WriteLine($"ProtectivePutModel: No puts added for {equity.Symbol} (quantity would be 0)");
                }
            }
            
            Console.WriteLine($"ProtectivePutModel: Processed {processedCount} equity positions, " +
                $"returning {riskAdjustedTargets.Count} risk-adjusted targets");
            
            return riskAdjustedTargets;
        }
    }
}
