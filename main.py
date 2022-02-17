from pm import Algo, Order, Miner

log_name = 'road'
Miner.generate_csv(log_name)
miner = Miner(log_name, Order.FRQ, Algo.IND, cut=100, top=5, filtering=False, frequency=False, update=True)
miner.process_stream()
miner.save_results()
miner.save_variant_histogram()
