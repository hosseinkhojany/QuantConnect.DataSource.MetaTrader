/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using System.Collections.Generic;
using System.Threading;
using MT;
using MtApi;
using MtApi5;
using MtProxyUI.SharedQC;
using NodaTime;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.HistoricalData;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.DataSource.MetaTrader
{
    
    public class MetaTraderDataProvider : SynchronizingHistoryProvider, IDataQueueHandler
    {
        
        private SymbolMapper symbolMapper = new();
        private EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;
        private List<SubscriptionDataConfig> SubscribedSymbols = [];
        private IDataAggregator aggregator;
        private Packets.LiveNodePacket job;
        private IAlgorithm algorithm;

        private IMT metaTrader;
        private MTType mtType = MTType.MT5;
        private string port = "8228";
        private bool _isInitialized;
        

        

        
        public bool IsConnected { get; }

        
        public MT_ENUM_TIMEFRAMES ConvertResolutionToMtTimeframe(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.Tick:
                    return MT_ENUM_TIMEFRAMES.PERIOD_CURRENT;
                case Resolution.Second:
                    return MT_ENUM_TIMEFRAMES.PERIOD_CURRENT;
                case Resolution.Minute:
                    return MT_ENUM_TIMEFRAMES.PERIOD_M1;
                case Resolution.Hour:
                    return MT_ENUM_TIMEFRAMES.PERIOD_H1;
                case Resolution.Daily:
                    return MT_ENUM_TIMEFRAMES.PERIOD_D1;
            }

            return MT_ENUM_TIMEFRAMES.PERIOD_CURRENT;
        }
        
        private bool AddSymbolSub(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var instrument in SubscribedSymbols)
            {
                metaTrader.AddSymbolToChart(SymbolMapper.ConvertLeanSymbolToOandaSymbol(instrument.Symbol.Value), ConvertResolutionToMtTimeframe(instrument.Resolution));
            }
            return true;
        }
        private bool RemoveSymbolSub(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var instrument in SubscribedSymbols)
            {
                metaTrader.RemoveSymbolFromChart(SymbolMapper.ConvertLeanSymbolToOandaSymbol(instrument.Symbol.Value), ConvertResolutionToMtTimeframe(instrument.Resolution));
            }
            return true;
        }
        public void OnPricingDataReceived(Object sender, Object data)
        {
            bool IsSymbolDataValid(string symbol)
            {
                bool founded = false;
                foreach (var t in SubscribedSymbols)
                {
                    if (t.Symbol.Value == symbol)
                    {
                        founded = true;
                        break;
                    }
                }

                return founded;
            }

            Log.Trace("OnPricingDataReceived()(MetaTrader):");
            if (data is Mt5QuoteEventArgs)
            {            
                Mt5QuoteEventArgs mt5QuoteArg = data as Mt5QuoteEventArgs;
                if (!IsSymbolDataValid(mt5QuoteArg.Quote.Instrument))
                {
                    return;
                }

                var securityType = symbolMapper.GetBrokerageSecurityType(mt5QuoteArg.Quote.Instrument);
                var symbol = symbolMapper.GetLeanSymbol(mt5QuoteArg.Quote.Instrument, securityType, Market.Oanda);
               
                aggregator.Update(new Tick(
                    mt5QuoteArg.Quote.Time,
                    symbol,
                    (decimal)mt5QuoteArg.Quote.Bid,
                    (decimal)mt5QuoteArg.Quote.Ask
                    ));
            }else if (data is MtQuoteEventArgs)
            {
                MtQuoteEventArgs mtQuoteArg = data as MtQuoteEventArgs;
                if (!IsSymbolDataValid(mtQuoteArg.Quote.Instrument))
                {
                    return;
                }
                var securityType = symbolMapper.GetBrokerageSecurityType(mtQuoteArg.Quote.Instrument);
                var symbol = symbolMapper.GetLeanSymbol(mtQuoteArg.Quote.Instrument, securityType, Market.Oanda);
               
                aggregator.Update(new Tick(
                    new DateTime(),
                    symbol,
                    (decimal)mtQuoteArg.Quote.Bid,
                    (decimal)mtQuoteArg.Quote.Ask
                ));
                
            }
        }
        private void Initialize()
        {
            if (_isInitialized)
            {
                return;
            }
            _isInitialized = true;
            aggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
                Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"),
                forceTypeNameOnExisting: false);
            if (mtType == MTType.MT5)
            {
                metaTrader = new MT5(true);
            }
            else
            {
                metaTrader = new MT4(true);
            }
            
            metaTrader.BeginConnect(port);
            Thread.Sleep(3000);
            metaTrader.QuoteUpdated += OnPricingDataReceived;
            
            SymbolMapper.setMtType(mtType);
            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            _subscriptionManager.SubscribeImpl += AddSymbolSub;
            _subscriptionManager.UnsubscribeImpl += RemoveSymbolSub;

        }
        public override void Initialize(HistoryProviderInitializeParameters parameters)
        { }

        public override IEnumerable<Slice> GetHistory(IEnumerable<HistoryRequest> requests, DateTimeZone sliceTimeZone)
        {
            var subscriptions = new List<Subscription>();
            foreach (var request in requests)
            {
                var history = GetHistory(request);

                if (history == null)
                {
                    continue;
                }

                var subscription = CreateSubscription(request, history);
                subscriptions.Add(subscription);
            }

            if (subscriptions.Count == 0)
            {
                return null;
            }

            return CreateSliceEnumerableFromSubscriptions(subscriptions, sliceTimeZone);
        }

        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {

            Logging.Log.Trace("Subscribe(MetaTrader):");
            if (!CanSubscribe(dataConfig.Symbol))
            {
                return null;
            }

            var enumerator = aggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManager.Subscribe(dataConfig);
            SubscribedSymbols.Add(dataConfig);

            return enumerator;
        }

        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionManager.Unsubscribe(dataConfig);
            aggregator.Remove(dataConfig);
            SubscribedSymbols.Remove(dataConfig);
        }

        public void SetJob(Packets.LiveNodePacket job)
        {
            Log.Trace("SetJob(MetaTrader):");
            this.job = job;
            Initialize();
        }
        public void Dispose()
        {
            aggregator?.DisposeSafely();
            _subscriptionManager?.DisposeSafely();
            throw new NotImplementedException();
        }

        private IEnumerable<BaseData> GetHistory(HistoryRequest request)
        {
            if (!CanSubscribe(request.Symbol))
            {
                return null;
            }

            throw new NotImplementedException();
        }
        
        private bool CanSubscribe(Symbol symbol)
        {
            if (symbol.Value.IndexOfInvariant("universe", true) != -1 || symbol.IsCanonical())
            {
                return false;
            }
            return true;
        }
    }
}
