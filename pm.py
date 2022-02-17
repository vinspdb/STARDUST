import subprocess
from platform import system
from sys import stdout
from enum import Enum
from collections import Counter
from datetime import timedelta
from os import path, makedirs, listdir
from matplotlib import pyplot
from pandas import DataFrame, read_csv
from time import process_time
from tempfile import TemporaryDirectory
from pm4py import read_bpmn, read_pnml
from pm4py.objects.log.obj import EventLog, Trace
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.conversion.bpmn import converter as bpmn_converter
from pm4py.objects.petri_net.exporter import exporter as pnml_exporter
from pm4py.streaming.importer.csv import importer as csv_stream_importer
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.evaluation.replay_fitness import algorithm as fitness_evaluator
from pm4py.algo.evaluation.precision import algorithm as precision_evaluator
from pm4py.visualization.petri_net import visualizer as pn_visualizer

CASE_ID_KEY = 'case:concept:name'
ACTIVITY_KEY = 'concept:name'
FINAL_ACTIVITY = '_END_'


class Algo(Enum):
    IND = 1
    SPL = 2
    ILP = 3


class Order(Enum):
    FRQ = 1
    MIN = 2
    MAX = 3


class Miner:

    @staticmethod
    def generate_csv(log_name, case_id=CASE_ID_KEY, activity=ACTIVITY_KEY, timestamp='time:timestamp'):
        """
        Converte il file XES in input in uno stream di eventi ordinati cronologicamente con formato CSV. Ogni traccia
        viene estesa con un evento conclusivo che ne definisca il termine
        :param log_name: nome del file XES (eventualmente compresso) contenente il log di eventi
        :param case_id: attributo identificativo dell'istanza di processo (con aggiunta del prefisso 'case:')
        :param activity: attributo identificativo dell'attività eseguita
        :param timestamp: attributo indicante l'istante di esecuzione di un evento
        """
        csv_path = path.join('eventlog', 'CSV', log_name + '.csv')
        if not path.isfile(csv_path):
            print('Generating CSV file from XES log...')
            xes_path = path.join('eventlog', 'XES', log_name)
            xes_path += '.xes.gz' if path.isfile(xes_path + '.xes.gz') else '.xes'
            log = xes_importer.apply(xes_path, variant=xes_importer.Variants.LINE_BY_LINE)
            for trace in log:
                trace.append({activity: FINAL_ACTIVITY, timestamp: trace[-1][timestamp] + timedelta(seconds=1)})
            dataframe = log_converter.apply(log, variant=log_converter.Variants.TO_DATA_FRAME)
            dataframe = dataframe.filter(items=[activity, timestamp, case_id]).sort_values(timestamp, kind='mergesort')
            dataframe = dataframe.rename(columns={activity: ACTIVITY_KEY, case_id: CASE_ID_KEY})
            makedirs(path.dirname(csv_path), exist_ok=True)
            dataframe.to_csv(csv_path, index=False)

    def __init__(self, log_name, order, algo, cut, top, filtering, frequency, update):
        """
        Metodo costruttore
        :param log_name: nome del file CSV contenente lo stream di eventi
        :param order: criterio di ordinamento delle varianti
        :param algo: algoritmo da utilizzare nell'apprendimento del modello
        :param cut: numero di istanze da esaminare preliminarmente
        :param top: numero di varianti da impiegare nella costruzione del modello (None per la distribuzione di Pareto)
        :param filtering: booleano per l'utilizzo di tecniche di filtering
        :param frequency: booleano per l'utilizzo delle frequenze nella costruzione del modello
        :param update: booleano per l'apprendimento dinamico del modello
        """
        self.log_name = log_name
        self.order = order
        self.algo = algo
        self.cut = cut
        self.top = top
        self.filtering = filtering
        self.frequency = frequency
        self.update = update
        self.processed_traces = 0
        self.variants = Counter()
        self.best_variants = None
        self.models = []
        self.drift_moments = []
        self.drift_variants = []
        self.evaluations = []

    def process_stream(self):
        """
        Processa iterativamente uno stream di eventi in formato CSV, ignorando attività che si ripetano in modo
        consecutivo per un numero di occorrenze superiore a due e aggiornando il contatore delle varianti in
        corrispondenza di un evento finale. Dopo aver esaminato un dato numero di istanze preliminari, viene generato
        un modello di processo. Tale modello sarà valutato su ciascuna delle istanze successive
        """
        print('Processing event stream...')
        stream = csv_stream_importer.apply(path.join('eventlog', 'CSV', self.log_name + '.csv'))
        traces = {}
        start = process_time()
        for event in stream:
            case = event[CASE_ID_KEY]
            activity = event[ACTIVITY_KEY]
            if activity == FINAL_ACTIVITY:
                new_trace = tuple(traces.pop(case))
                self.variants[new_trace] += 1
                self.processed_traces += 1
                if self.processed_traces == self.cut:
                    self.select_best_variants()
                    self.learn_model()
                    end = process_time()
                    self.evaluations.append([None, None, None, end - start])
                    start = end
                elif self.processed_traces > self.cut:
                    stdout.write(f'\rCurrent model: {len(self.models)}\tCurrent trace: {self.processed_traces}')
                    self.evaluate_model(new_trace)
                    if self.update:
                        self.select_best_variants()
                        if self.best_variants.keys() != self.drift_variants[-1].keys():
                            self.learn_model()
                    end = process_time()
                    self.evaluations[-1].append(end - start)
                    start = end
            elif case not in traces:
                traces[case] = [activity]
            elif len(traces[case]) == 1 or traces[case][-1] != activity or traces[case][-2] != activity:
                traces[case].append(activity)

    def select_best_variants(self):
        """
        Determina le varianti più significative all'istante corrente secondo il criterio d'ordine selezionato
        """
        top_variants = self.top
        if top_variants is None:
            counter = 0
            top_variants = 0
            while counter / self.processed_traces < 0.8:
                counter += sorted(self.variants.values(), reverse=True)[top_variants]
                top_variants += 1
        if self.order == Order.FRQ:
            self.best_variants = {item[0]: item[1] for item in self.variants.most_common(top_variants)}
        else:
            candidate_variants = list(item[0] for item in self.variants.most_common())
            candidate_variants.sort(key=lambda v: len(v), reverse=self.order == Order.MAX)
            self.best_variants = {var: self.variants[var] for var in candidate_variants[:top_variants]}

    def learn_model(self):
        """
        Genera un modello di processo utilizzando le varianti più significative all'istante corrente
        """
        log = EventLog()
        for variant, occurrence in self.best_variants.items():
            for i in range(occurrence if self.frequency else 1):
                log.append(Trace({ACTIVITY_KEY: activity} for activity in variant))
        if self.algo == Algo.IND:
            variant = inductive_miner.Variants.IMf if self.filtering else inductive_miner.Variants.IM
            model = inductive_miner.apply(log, variant=variant)
        else:
            with TemporaryDirectory() as temp:
                log_path = path.join(temp, 'log.xes')
                variant = xes_exporter.Variants.LINE_BY_LINE
                xes_exporter.apply(log, log_path, variant, {variant.value.Parameters.SHOW_PROGRESS_BAR: False})
                model_path = path.join(temp, 'model.bpmn' if self.algo == Algo.SPL else 'model.pnml')
                script = path.join('scripts', 'run.bat' if system() == "Windows" else 'run.sh')
                args = (script, self.algo.name, str(self.filtering), log_path, path.splitext(model_path)[0])
                subprocess.call(args, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
                model = bpmn_converter.apply(read_bpmn(model_path)) if self.algo == Algo.SPL else read_pnml(model_path)
        self.models.append(model)
        self.drift_moments.append(self.processed_traces)
        self.drift_variants.append(self.best_variants)

    def evaluate_model(self, trace):
        """
        Valuta il modello di processo sull'istanza fornita in input
        :param trace: istanza di processo da impiegare nella valutazione
        """
        log = EventLog([Trace({ACTIVITY_KEY: activity} for activity in trace)])
        variant = fitness_evaluator.Variants.ALIGNMENT_BASED
        fitness = fitness_evaluator.apply(log, *self.models[-1], variant=variant)['average_trace_fitness']
        variant = precision_evaluator.Variants.ALIGN_ETCONFORMANCE
        parameters = {variant.value.Parameters.SHOW_PROGRESS_BAR: False}
        precision = precision_evaluator.apply(log, *self.models[-1], variant=variant, parameters=parameters)
        f_measure = 2 * fitness * precision / (fitness + precision) if fitness != 0 else 0
        self.evaluations.append([fitness, precision, f_measure])

    def compute_model_complexity(self, index):
        """
        Calcola la complessità del modello indicizzato
        :param index: indice del modello di cui calcolare la complessità
        :return: numero di posti, numero di transizioni, numero di archi e metrica "Extended Cardoso" del modello
        """
        net = self.models[index][0]
        ext_card = 0
        for place in net.places:
            successor_places = set()
            for place_arc in place.out_arcs:
                successors = frozenset(transition_arc.target for transition_arc in place_arc.target.out_arcs)
                successor_places.add(successors)
            ext_card += len(successor_places)
        return len(net.places), len(net.transitions), len(net.arcs), ext_card

    def save_results(self):
        """
        Esporta report, valutazioni e modelli di processo
        """
        print('\nExporting results...')
        filtering = 'UFL' if self.filtering else 'NFL'
        frequency = 'UFR' if self.frequency else 'NFR'
        update = 'D' if self.update else 'S'
        top_variants = 'P' if self.top is None else self.top
        file = f'{self.order.name}.{self.algo.name}.{self.cut}.{top_variants}.{filtering}.{frequency}.{update}'
        folder = path.join('results', self.log_name, 'report')
        makedirs(folder, exist_ok=True)
        top_variants = max(len(variants.keys()) for variants in self.drift_variants)
        columns = ['trace', 'places', 'transitions', 'arcs', 'ext_cardoso',
                   *[f'trace_{i}' for i in range(1, top_variants + 1)]]
        report = DataFrame(columns=columns)
        report.index.name = 'n°_training'
        for index, current_variants in enumerate(self.drift_variants):
            traces = [f'[{v}]{k}' if self.order == Order.FRQ else f'[{len(k)}:{v}]{k}' for k, v in
                      current_variants.items()] + [None] * (top_variants - len(current_variants))
            report.loc[len(report)] = [self.drift_moments[index], *self.compute_model_complexity(index), *traces]
        report.to_csv(path.join(folder, file + '.csv'))
        folder = path.join('results', self.log_name, 'evaluation')
        makedirs(folder, exist_ok=True)
        columns = ['fitness', 'precision', 'f-measure', 'time']
        evaluation = DataFrame(self.evaluations, columns=columns)
        evaluation.index.name = 'n°_evaluation'
        total_time = evaluation['time'].sum()
        evaluation.loc['AVG'] = evaluation.mean()
        evaluation.loc['TOT'] = [None, None, None, total_time]
        evaluation.to_csv(path.join(folder, file + '.csv'))
        folder = path.join('results', self.log_name, 'petri')
        makedirs(folder, exist_ok=True)
        for index, model in enumerate(self.models):
            model_info = f'-{index}' if self.update else ''
            pnml_exporter.apply(model[0], model[1], path.join(folder, file + model_info + '.pnml'), model[2])
            pn_visualizer.save(pn_visualizer.apply(*model), path.join(folder, file + model_info + '.png'))

    def save_variant_histogram(self, y_log=False):
        """
        Esporta l'istogramma delle varianti di processo
        :param y_log: booleano per l'utilizzo di una scala logaritmica sull'asse delle ordinate
        """
        file = ('frequency' if self.order == Order.FRQ else 'length') + '_histogram.png'
        folder = path.join('results', self.log_name)
        makedirs(folder, exist_ok=True)
        if not path.isfile(path.join(folder, file)):
            y_axis = self.variants.values() if self.order == Order.FRQ else [len(v) for v in self.variants.keys()]
            pyplot.bar(range(1, len(self.variants) + 1), y_axis)
            pyplot.title(f'Traces processed: {self.processed_traces}     Variants found: {len(self.variants)}\n')
            pyplot.xlabel('Variants')
            pyplot.ylabel('Frequency' if self.order == Order.FRQ else 'Length')
            if y_log:
                pyplot.semilogy()
            pyplot.savefig(path.join(folder, file))

    @staticmethod
    def generate_summary(log_name):
        """
        Genera una visualizzazione sintetica dei risultati ottenuti
        :param log_name: nome del log per il quale generare un sommario dei risultati
        """
        folder = path.join('results', log_name)
        if not path.isdir(folder):
            print('No results found')
            return
        print('Generating summary...')
        error_file = path.join(folder, 'errors.csv')
        errors = read_csv(error_file) if path.isfile(error_file) else DataFrame(columns=['algo'])
        columns = ['order', 'top-variants', 'set-up', 'fitness', 'precision', 'f-measure', 'time', 'ext_cardoso']
        for algo in Algo:
            summary = DataFrame(columns=columns)
            for error in errors.loc[errors['algo'] == algo.name].itertuples():
                row = [error.order, error.top, f'{error.filtering} {error.frequency} {error.update}'] + ['-'] * 5
                summary.loc[len(summary)] = row
            for file in listdir(path.join(folder, 'evaluation')):
                if algo.name in file:
                    parameters = file.split(sep='.')
                    row = [parameters[0], parameters[3], f'{parameters[4]} {parameters[5]} {parameters[6]}']
                    evaluation = read_csv(path.join(folder, 'evaluation', file), dtype={'n°_evaluation': 'str'})
                    row.extend(evaluation[val][len(evaluation) - 2] for val in ('fitness', 'precision', 'f-measure'))
                    row.append(evaluation['time'][len(evaluation) - 1])
                    report = read_csv(path.join(folder, 'report', file))
                    row.append(str(report['ext_cardoso'].tolist()))
                    summary.loc[len(summary)] = row
            summary['top-variants'] = summary['top-variants'].replace('P', -1).astype(int)
            summary = summary.sort_values(['order', 'top-variants', 'set-up'], ignore_index=True)
            summary['top-variants'] = summary['top-variants'].replace(-1, 'P')
            summary.index.name = 'experiment'
            summary.index += 1
            summary.to_csv(path.join(folder, algo.name + '.csv'))
