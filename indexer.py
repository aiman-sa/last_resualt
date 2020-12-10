from MapReduce import MapReduce
from parser_module import Parse
import psutil
import sys
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import concurrent.futures


class Indexer:

    def __init__(self, config, all_terms_dict):
        self.inverted_idx = all_terms_dict
        #self.postingDict = {}
        self.fileName = 'InvertedIndex'
        self.config = config
        # {term: [ordered list where appear : (file_id , lineNumber)]}
        self.thread_pool_size = 1
        avg_ram = (psutil.virtual_memory().available // 5)//(self.thread_pool_size +1)
        path = 'MapReduceData/'
        self.avg_length =(avg_ram // sys.getsizeof((int(), str()))) // (8/10)
        # self.map_reduce = MapReduce(self.avg_length,self.thread_pool_size)
        self.map_reduce_ag = MapReduce(self.avg_length, self.thread_pool_size, path + 'AG/')
        self.map_reduce_hq = MapReduce(self.avg_length, self.thread_pool_size, path + 'HQ/')
        self.map_reduce_rz = MapReduce(self.avg_length, self.thread_pool_size, path + 'Rz/')
        self.map_reduce_other = MapReduce(self.avg_length, self.thread_pool_size, path + 'Others/')
        self.map_reduce_doc = MapReduce(self.avg_length, self.thread_pool_size, path + 'Document/')
        #self.tmp_pos = {}
        # self.num_in_pos_tmp = 0
        self.num_in_pos_ag_tmp = [0]
        self.num_in_pos_hq_tmp = [0]
        self.num_in_pos_rz_tmp = [0]
        self.num_in_pos_other_tmp = [0]
        self.num_in_pos_doc_other = [0]
        self.Entitys = {}
        self.tmp_pos_ag = {}
        self.tmp_pos_hq = {}
        self.tmp_pos_rz = {}
        self.tmp_pos_other = {}
        self.tmp_pos_doc = {}
        # self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self.NUMBER_OF_PROCESSES = 5
        self.set_is_writting = {}

    def get_right_tmp_pos_and_num(self, first_letter):
        lower_letter = str(first_letter).lower()
        if 'a' <= lower_letter <= 'g':
            return [self.tmp_pos_ag, self.num_in_pos_ag_tmp, self.map_reduce_ag, 'AG']
        elif 'h' <= lower_letter <= 'q':
            return [self.tmp_pos_hq, self.num_in_pos_hq_tmp, self.map_reduce_hq, 'HQ']
        elif 'r' <= lower_letter <= 'z':
            return [self.tmp_pos_rz, self.num_in_pos_rz_tmp, self.map_reduce_rz, 'Rz']
        return [self.tmp_pos_other, self.num_in_pos_other_tmp, self.map_reduce_other, 'Others']


    def save_left_over(self, dict,map_reduce):
        map_reduce.write_dict_func(dict)
        map_reduce.wait_untill_finish()

    def save_all_map_reduce(self):
        self.map_reduce_ag.save_map_reduce()
        self.map_reduce_hq.save_map_reduce()
        self.map_reduce_rz.save_map_reduce()
        self.map_reduce_other.save_map_reduce()
        self.map_reduce_doc.save_map_reduce()

    def check_save_left_over_ag(self):
        if self.num_in_pos_ag_tmp[0] > 0:
            self.save_left_over(self.tmp_pos_ag, self.map_reduce_ag)
            self.num_in_pos_ag_tmp[0] = 0
            #print('Waiting to map_reduce_ag')
            self.map_reduce_ag.wait_untill_finish()
            #print('Fisihed waiting to map_reduce_ag')

    def check_save_left_over_hq(self):
        if self.num_in_pos_hq_tmp[0] > 0:
            self.save_left_over(self.tmp_pos_hq, self.map_reduce_hq)
            self.num_in_pos_hq_tmp[0] = 0
            #print('Waiting to map_reduce_hq')
            self.map_reduce_hq.wait_untill_finish()
            #print('Fisihed waiting to map_reduce_hq')

    def check_save_left_over_rz(self):
        if self.num_in_pos_rz_tmp[0] > 0:
            self.save_left_over(self.tmp_pos_rz, self.map_reduce_rz)
            self.num_in_pos_rz_tmp[0] = 0
            #print('Waiting to map_reduce_rz')
            self.map_reduce_rz.wait_untill_finish()
            #print('Fisihed waiting to map_reduce_rz')

    def check_save_left_over_others(self):
        if self.num_in_pos_other_tmp[0] > 0:
            self.save_left_over(self.tmp_pos_other,self.map_reduce_other)
            self.num_in_pos_other_tmp[0] = 0
            #print('Waiting to map_reduce_rz')
            self.map_reduce_other.wait_untill_finish()
            #print('Fisihed waiting to map_reduce_rz')

    def check_save_left_over_doc(self):
        if self.num_in_pos_doc_other[0] > 0:
            self.save_left_over(self.tmp_pos_doc, self.map_reduce_doc)
            self.num_in_pos_doc_other[0] = 0
            #print('Waiting to map_reduce_doc')
            self.map_reduce_doc.wait_untill_finish()
            #print('Fisihed waiting to map_reduce_doc')

    def save_all_left_overs(self):
        with ProcessPoolExecutor() as process_exector:
            process_exector.map(self.check_save_left_over_ag())
            process_exector.map(self.check_save_left_over_hq())
            process_exector.map(self.check_save_left_over_rz())
            process_exector.map(self.check_save_left_over_others())
            process_exector.map(self.check_save_left_over_doc())

    def get_total_size(self):
        return len(self.map_reduce_ag.meta_data) + len(self.map_reduce_hq.meta_data)+ len(self.map_reduce_rz.meta_data) + len(self.map_reduce_other.meta_data) + len(self.map_reduce_ag.meta_data)

    def addEntitysToPosting(self, term, tweet_id, quantity):
        #str_term = str(term)
        first_letter = [0]
        tmp_pos, number_arr, map_reduce, _ = self.get_right_tmp_pos_and_num(first_letter)
        if term.upper() not in self.Entitys.keys() and term.upper() not in tmp_pos.keys():  # Entitys=>{"DONALD TRUMP:(12,3)},inverted_idx=> {DONALD TRUMP}
            self.Entitys[term.upper()] = (tweet_id, quantity)
        else:
            if term.upper() not in self.inverted_idx.keys():
                self.inverted_idx[term.upper()] = 2
                if term.upper() not in tmp_pos.keys():
                    tmp_pos[term.upper()] = []
                tmp_pos[term.upper()].append(self.Entitys[term.upper()])
                tmp_pos[term.upper()].append((tweet_id, quantity))
                number_arr[0] += 2
            else:
                self.inverted_idx[term.upper()] += 1
                if term.upper() not in tmp_pos.keys():
                    tmp_pos[term.upper()] = []
                tmp_pos[term.upper()].append((tweet_id, quantity))
                number_arr[0] += 1


    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """
        document_dictionary = document.term_doc_dictionary #{term:freq,term:freq}
        term_lst = [*document_dictionary]
        term_lst.sort(key=lambda x: x.lower())
        for i in range(len(term_lst)):
            term = term_lst[i]
            tmp_pos, number_arr, map_reduce,key = self.get_right_tmp_pos_and_num(term[0])
            # try:
            if term[0].isupper() and " " in term:
                self.addEntitysToPosting(term, document.tweet_id, document_dictionary[term])
                continue
            if number_arr[0] >= self.avg_length:
                map_reduce.write_dict(tmp_pos)
                self.set_is_writting.add(key)
                number_arr[0] = 0
            if term.lower() not in tmp_pos.keys():
                tmp_pos[term.lower()] = []
            if key in self.set_is_writting:
                map_reduce.wait_untill_finish()
                self.set_is_writting.remove(key)
            tmp_pos[term.lower()].append((document.tweet_id, document_dictionary[term]))
            number_arr[0] += 1
            # except:
            #     print('TERMS : _____ ' + str(term))
            #     print('INVERTED: problem with the following key {}'.format(term[0]))
        max_freq = max([document_dictionary.values()])
        self.tmp_pos_doc[document.tweet_id] = document_dictionary
        self.num_in_pos_doc_other[0] += 1
        if self.num_in_pos_doc_other[0] >= self.avg_length:
            self.map_reduce_doc.write_dict(self.tmp_pos_doc)
            self.num_in_pos_doc_other[0] = 0


