using System;
using System.Collections.Generic;
using System.Linq;
using QuantConnect;
using QuantConnect.Algorithm.Framework.Selection;
using QuantConnect.Data.Fundamental;
using QuantConnect.Algorithm;

namespace QuantConnect
{
    public class BasicUniverseSelectionModel : FundamentalUniverseSelectionModel
    {
        public const int PNumCoarse = 200;
        public const int PNumFine = 70;

        private int _numCoarse;
        private int _numFine;
        private HashSet<string> _restrictedISINs;
        private bool _isInitialized = false;
        private QCAlgorithm _algorithm;

        public BasicUniverseSelectionModel(int numCoarse = PNumCoarse, int numFine = PNumFine)
        {
            this._numCoarse = numCoarse;
            this._numFine = numFine;
            this._restrictedISINs = new HashSet<string>();
        }

        private void Initialize(QCAlgorithm algorithm)
        {
            if (_isInitialized) return;
            
            _algorithm = algorithm;
            
            try
            {
                // Load the CSV file using the ObjectStore
                var csvContent = algorithm.ObjectStore.ReadString("kpmgrestricted.csv");
                if (string.IsNullOrEmpty(csvContent))
                {
                    algorithm.Log("Error: Could not read kpmgrestricted.csv from ObjectStore");
                    
                    // Try to use the Download method
                    try {
                        csvContent = algorithm.Download("kpmgrestricted.csv");
                        algorithm.Log("Successfully downloaded kpmgrestricted.csv");
                    }
                    catch (Exception ex) {
                        algorithm.Log($"Download attempt failed: {ex.Message}");
                        _isInitialized = true;
                        return;
                    }
                }
                
                if (string.IsNullOrEmpty(csvContent))
                {
                    algorithm.Log("Failed to load kpmgrestricted.csv by any method");
                    _isInitialized = true;
                    return;
                }
                
                var lines = csvContent.Split('\n');
                
                // Skip header line and extract ISINs
                for (int i = 1; i < lines.Length; i++)
                {
                    var line = lines[i].Trim();
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    
                    var columns = line.Split(',');
                    if (columns.Length >= 2 && !string.IsNullOrWhiteSpace(columns[1]))
                    {
                        // Clean the ISIN before adding it - remove quotes and normalize case
                        var isin = columns[1].Trim().Trim('"').ToUpperInvariant();
                        _restrictedISINs.Add(isin);
                    }
                }
                
                algorithm.Log($"Loaded {_restrictedISINs.Count} restricted ISINs from CSV file");
                
                // Log some sample ISINs to verify loading
                if (_restrictedISINs.Count > 0)
                {
                    var sampleIsins = string.Join(", ", _restrictedISINs.Take(5).Select(isin => $"\"{isin}\""));
                    algorithm.Log($"Sample ISINs: {sampleIsins}");
                }
                
                // Test a few known symbols for ISIN lookup
                var testSymbols = new[] { "AAPL", "MSFT", "GOOGL", "AMZN", "SPY" };
                foreach (var sym in testSymbols)
                {
                    try
                    {
                        var symbol = Symbol.Create(sym, SecurityType.Equity, Market.USA);
                        var isin = algorithm.ISIN(symbol);
                        if (!string.IsNullOrEmpty(isin))
                        {
                            bool isRestricted = _restrictedISINs.Contains(isin.Trim().ToUpperInvariant());
                            algorithm.Log($"Test ISIN lookup: {sym} -> {isin} (Restricted: {isRestricted})");
                        }
                        else
                        {
                            algorithm.Log($"Test ISIN lookup: {sym} -> No ISIN returned");
                        }
                    }
                    catch (Exception ex)
                    {
                        algorithm.Log($"ISIN lookup failed for {sym}: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                // Log error but continue with empty restricted list
                algorithm.Log($"Error loading restricted ISINs: {ex.Message}");
            }
            
            _isInitialized = true;
        }

        public override IEnumerable<Symbol> Select(QCAlgorithm algorithm, IEnumerable<Fundamental> fundamental)
        {
            // Initialize if not already done
            if (!_isInitialized)
            {
                Initialize(algorithm);
            }
            
            var selected = fundamental;
            int totalCount = selected.Count();
            
            // Apply filter for HasFundamentalData and Price > 5
            var filtered = selected
                .Where(f => f.HasFundamentalData && f.Price > 5)
                .ToList();
            
            // Sort by dollar volume for coarse selection
            var sortedByDollarVolume = filtered
                .OrderByDescending(f => f.DollarVolume)
                .ToList();
            
            // Collect enough non-restricted stocks for coarse selection
            var coarseSelection = new List<Fundamental>();
            int processedCount = 0;
            int restrictedCount = 0;
            
            // Process stocks until we get _numCoarse non-restricted stocks or run out of stocks
            while (coarseSelection.Count < _numCoarse && processedCount < sortedByDollarVolume.Count)
            {
                var item = sortedByDollarVolume[processedCount++];
                bool isRestricted = false;
                
                // Try to get the ISIN for this symbol and check if it's in the restricted list
                try
                {
                    var isin = algorithm.ISIN(item.Symbol);
                    if (!string.IsNullOrEmpty(isin))
                    {
                        // Normalize the ISIN for comparison
                        isin = isin.Trim().ToUpperInvariant();
                        
                        // Check if it's in our restricted set
                        if (_restrictedISINs.Contains(isin))
                        {
                            isRestricted = true;
                            restrictedCount++;
                            
                            // Log restricted symbols (limit to avoid flooding logs)
                            if (restrictedCount <= 5 || restrictedCount % 20 == 0)
                            {
                                algorithm.Log($"Filtered out restricted symbol {item.Symbol.Value} with ISIN {isin}");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // If we can't get the ISIN, assume it's not restricted
                    if (processedCount <= 5)
                    {
                        algorithm.Log($"Couldn't get ISIN for {item.Symbol.Value}: {ex.Message}");
                    }
                }
                
                if (!isRestricted)
                {
                    coarseSelection.Add(item);
                }
            }
            
            // Log stats on the first day of the month
            if (algorithm.Time.Day == 1)
            {
                algorithm.Log($"Universe selection stats: Total {totalCount}, Processed {processedCount}, Restricted {restrictedCount}, Selected {coarseSelection.Count}");
            }
            
            // Log if we couldn't get enough non-restricted stocks
            if (coarseSelection.Count < _numCoarse && restrictedCount > 0)
            {
                algorithm.Log($"Warning: Only found {coarseSelection.Count} non-restricted stocks out of desired {_numCoarse}. Filtered out {restrictedCount} restricted stocks.");
            }
            
            // Final fine selection - order by market cap and take top symbols
            var fineSelection = coarseSelection
                .OrderByDescending(f => f.MarketCap)
                .Take(Math.Min(_numFine, coarseSelection.Count));
            
            return fineSelection.Select(f => f.Symbol);
        }
    }
}