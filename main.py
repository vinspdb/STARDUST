from pm import Algo, Order, Miner

log_name = 'road'
Miner.generate_csv(log_name)
for order, algo, cut, top, filtering, frequency, update in product([Order.FRQ], Algo, [15000], [None], [True, False], [True, False], [True, False]):
    print(f'{order.name},{algo.name},{cut},{top},{filtering},{frequency},{update}')
    miner = Miner(log_name, order, algo, cut, top, filtering, frequency, update)
    try:
        miner.process_stream_with_tree()
        miner.save_results()
        miner.generate_summary(log_name)
    except Exception as e:
        print(f'\tError: {e}')
