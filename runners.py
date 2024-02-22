from .iterators import *
from .utils import *
from .file_io import *
import logging

        
class HQLRunner():
    """
    A class to format and run HQL Queries in a Spark Engine.

    Attributes
    ----------

    hql_files: iterable
        either a single HQLQuery or an instance of iterable. Alternatively, you specify a filenames by string.

    is_temp: boolean
        optional boolean parameter to force all HQLQueries to be saved as local temp tables. 

    Methods 
    -------
    run(run_settings, *iterator=None):
        Formats HQL Files with run_settings parameters and can use an iterator(s) to loop through different dates 
        Iterators CAN specify different repetition amounts although its not recommended per business precedent, but its possible
            a certain backfill failed at a certain iteration step for a certain iterator
    
    """
    def __init__(self, hql_queries, is_temp=None):

        hql_queries = [hql_queries] if type(hql_queries) is not list else hql_queries 
        self.hql_queries = []
        for query in hql_queries:
            if type(query) is str: # If user provided filename string rather than HQLQuery convert
                self.hql_queries.append(HQLQuery(query))
            elif type(query) is HQLQuery:
                self.hql_queries.append(query)
    
        # Set all HQLQueries to temp tables based on is_temp above
        if is_temp is not None:
            for hql_query in self.hql_queries:
                # Use the is_temp flag if you want to override the "is_temp" field for all the HQLQueries. 
                hql_query.is_temp = is_temp


    def run(self, run_settings = None, iterators = None):
        
        """
        Main Runner method that passess a formatted HQL Query through a spark engine. 

        Parameters
        ----------
        run_settings: dictionary
            Dictionary used to format variables in a query string. Used in all "run" methods.  

        iterator: MvnoIterator
            Takes a possible MvnoIterator(s) and uses its next() functionality to loop through dates returning a dictionary of formatted date variables

        """
        self.done_iterate = 0
        if iterators:
            while self.iterate(run_settings, iterators):
                continue

        else:
            for hqlQuery in self.hql_queries:
                hqlQuery.run(run_settings)

    def iterate(self, run_settings = None, iterators = None):
        assert iterators is not None, "ERROR: Supply an iterator (BillCycleIterator, CalendarMonthIterator, etc.) "
        
        if not hasattr(self, "done_iterate"):
            self.done_iterate = 0

        iterators = [ iterators] if type(iterators) is not list else iterators 
        if self.done_iterate < len(iterators):
            for iterator in iterators: #First pick an iterator
                iterator.logging = False
                if next(iterator, None) == None: #Added complexity for the case that iterators have differing iterations (should be rare)
                    self.done_iterate += 1 
                    if self.done_iterate == len(iterators):
                        del self.done_iterate
                    return
                
                for hqlQuery in self.hql_queries: #Then use that iterator to go over every HQLQuery
                    run_settings.update(iterator.date_dict)
                    logging.info(f"HQL Runner {hqlQuery.table_name} \n\t Executing Date Range: {iterator.__class__.__name__} ({run_settings['start_date']}, {run_settings['end_date']})")
                    logging.info(iterator.date_dict)
                    hqlQuery.run(run_settings)
            return True

        else:
            return
