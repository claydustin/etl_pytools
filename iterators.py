
import datetime
from collections.abc import Iterator
from dateutil.relativedelta import relativedelta
import logging
from MVNO_python_tools.mvno_etl_tools import find_fiscal

TODAY = datetime.date.today()

class BackFillIterator(Iterator):
    """
    Abstract base class. A type of iterator for MVNO use which specifically uses a dictionary to format HQL queries with dates. 
    Allows user to iterate through a range of dates by specifying a start date and iterations along with a directional parameter 
        to control the direction of the iteration. 

    Attributes
    ----------
    iter_int: int
        -1 or 1 integer that indicates the direction of iterations movement. -1 constant moves the iteration backwards in time
    
    iter_count: int
        iteration's counter. Used in loop to check when to stop

    iterations: int
        number of iterations requested

    start_date: date
        date object converted from user defined string of date. 

    end_date: date
        date object used to define the ending range for interval based queries (i.e. [start_date, end_date])

    backwards: bool
        Specifies the direction of the backfill. Current implementation has forward backfill starting on the "start_date" and iterating up

    Methods 
    -------
    update_dict_dict(additional_params=dict):
        Takes additional params from a dictionary saves the class date variables into a dictionary
    
    """
    def __init__(self, start_date = TODAY, iterations = 0, backwards=True, logging=True, is_complete=False, iterator_abbrev=None, expand_days=[1,1]):
        
        #Define a constant multiplier that determines direction of iteration
        self.iter_int = -1 if backwards else 1
        self.iter_count = 0
        self.iterations = iterations
        self.start_date = start_date
        self.expand_days = expand_days
        self.iterator_abbrev = iterator_abbrev

        ##Convert Date Strings to datetime objects
        if type(start_date) == str:
            self.start_date = datetime.datetime.strptime(self.start_date, "%Y-%m-%d").date()

        if is_complete & backwards:
            self.logging=False
            self._next_complete_month()

        self.logging = logging
        # End_date only gets set at first "next" or first iteration
        self.end_date = None


    def update_date_dict(self, additional_params = {}):
        """
        Allows us to add parameters that we wanted changed after every iteration. 
        Maybe in the future we want dates for other time zones. 

        Entries to the date_dict variable will format the eventual HQL string or date ranges for queries

        Parameters
        ----------
        additional_params:
            Optional dictionary of values to assign to date_dict. Items specific to a child of the BackFillIterator class. 
            ex. {"BillCycleMonthString": "May"}
        """
        
        date_dict = {}
        #Format dates in ET
        date_dict['start_date'] = self.start_date
        date_dict['end_date']   = self.end_date

        #Format dates extensions
        date_dict['start_date_expanded'] = self.start_date - datetime.timedelta(days=self.expand_days[0])
        date_dict['end_date_expanded']   = self.end_date + datetime.timedelta(days=self.expand_days[1])

        #Values that help with
        date_dict["month_name"] = self.start_date.strftime("%B")
        date_dict["year_month"] = self.start_date.strftime("%Y-%m")

        #bill cycle dates based on the start and end dates - must start the prior month (specific to mvno cube so far)
        date_dict['bill_cycle_start_date'] = (self.start_date - relativedelta(months=1)).replace(day=1)
        date_dict['bill_cycle_end_date']   = self.end_date.replace(day=1)

        date_dict["iterator_type"] = self.iterator_abbrev

        date_dict.update(additional_params)
        if self.logging == True:
            logging.info(f"\nIterator {self.__class__.__name__} {date_dict['year_month']} \n\t| Iteration {date_dict['month_name']} \n\t| Range [{self.start_date}, {self.end_date}]")
        
        self.date_dict = date_dict


    def _next_complete_month(self):
        start_date = self.start_date
        while start_date < self.peek():
            self.__next__()
            self.iter_count -= 1


class CalendarMonthIterator(BackFillIterator):

    def __init__(self, start_date = TODAY, iterations = 0, backwards=True, logging=True, is_complete=False, expand_days=[1,1]):
        super().__init__(start_date, iterations, backwards, logging, is_complete, iterator_abbrev="cal", expand_days=expand_days)
        ## Month Iterators have start date beginning at first of month
        

    def __iter__(self):
        return self


    def peek(self):
        start_date = self.start_date.replace(day=1)

        # Sets the end date to the last date of the start-date's month. 
        end_date     = start_date + relativedelta(months=1) - datetime.timedelta(days=1)

        return end_date


    def __next__(self):
        self.start_date = self.start_date.replace(day=1)

        if self.iter_count < self.iterations:

            # Sets the end date to the last date of the start-date's month. 
            self.end_date     = self.start_date + relativedelta(months=1) - datetime.timedelta(days=1)

            # Collects all important date parameters into a dictionary 
            self.update_date_dict()

            # Increment for next iteration
            self.iter_count += 1    
            self.start_date += relativedelta(months=self.iter_int) #iter_int is either -1 or 1

            return self.date_dict


class BillCycleIterator(BackFillIterator):

    def __init__(self, start_date = TODAY, iterations = 0, backwards = True, logging=True, is_complete=False, expand_days=[1,1]):
        super().__init__(start_date, iterations, backwards, logging, is_complete, iterator_abbrev="bill-cycle", expand_days=expand_days)

    def __iter__(self):
        return self

    
    def peek(self):
        start_date = self.start_date.replace(day=1)    
            # Sets the end date to the 28 of the next month from the start date
        end_date     = (start_date + relativedelta(months=1)).replace(day=28)
    
        return end_date


    def __next__(self):
        self.start_date = self.start_date.replace(day=1)

        if self.iter_count < self.iterations:
            
            # Sets the end date to the 28 of the next month from the start date
            self.end_date     = (self.start_date + relativedelta(months=1)).replace(day=28)

            # Collects all important date parameters into a dictionary   
            self.update_date_dict()

            # Increment for next iteration
            self.iter_count += 1    
            self.start_date += relativedelta(months=self.iter_int) #iter_int is either -1 or 1
            
            return self.date_dict


class FiscalMonthIterator(BackFillIterator):

    def __init__(self, start_date = TODAY, iterations = 0, backwards = True, logging=True, is_complete=False, expand_days=[1,1]):
        super().__init__(start_date, iterations, backwards, logging, is_complete, iterator_abbrev="fis", expand_days=expand_days)
        ## Fiscal Month of year will be represented as a date but really we just use the "YY-MM" designation to 
        ##   offer input to the "find_fiscal"         
    def __iter__(self):
        return self


    def peek(self):
        if self.iter_count == 0:
            fiscal_moy = self.start_date
        else:
            fiscal_moy = self.fiscal_moy

        fiscal_start_end = find_fiscal(fiscal_moy.year, fiscal_moy.month)
        end_date     = fiscal_start_end["fis_end"]
    
        return end_date


    def __next__(self):
        if self.iter_count == 0:
            self.fiscal_moy = self.start_date
        fiscal_start_end = find_fiscal(self.fiscal_moy.year, self.fiscal_moy.month)
        
        if self.iter_count < self.iterations:
            self.end_date     = fiscal_start_end["fis_end"]
            self.start_date   = fiscal_start_end["fis_start"]
            
            # Collects all important date parameters into a dictionary   
            #   Set additional params according to the Fiscal Month and not the Start Date as is the default
            self.update_date_dict(additional_params={"year_month": self.fiscal_moy.strftime("%Y-%m"), "month_name": self.fiscal_moy.strftime("%B")})

            # Increment for next iteration
            self.iter_count += 1    
            
            self.fiscal_moy += relativedelta(months=self.iter_int) #iter_int is either -1 or 1 depending on "backwards"/ direction

            return self.date_dict


class DailyIterator(BackFillIterator):

    def __init__(self, start_date = TODAY, iterations = 0, backwards = True, logging=True, expand_days=[1,1]):
        super().__init__(start_date, iterations, backwards, logging, iterator_abbrev="daily", expand_days=expand_days)

    def __iter__(self):
        return self

    def peek(self):
        return self.start_date


    def __next__(self):
        if self.iter_count < self.iterations:
            
            self.end_date     = self.start_date

            # Collects all important date parameters into a dictionary   
            self.update_date_dict()

            # Increment for next iteration
            self.iter_count += 1    
            self.start_date += relativedelta(days=self.iter_int) #iter_int is either -1 or 1

            return self.date_dict


class MultiDayIterator(BackFillIterator):
    """
    Like the DailyIterator, but allows for chunks of days at a time
    If either LOOPS or DAYS_TO_LOAD are zero, the loop will immediately abort

    Parameters
    ----------
    additional_params:
        Optional dictionary of values to assign to date_dict. Items specific to a child of the BackFillIterator class. 
        ex. {"LOOPS": 2, "DAYS_TO_LOAD": 14}
    """

    def __init__(self, start_date = TODAY, iterations = 0, backwards = True, logging=True, days = 1, expand_days=[1,1]):
        super().__init__(start_date, iterations, backwards, logging, iterator_abbrev="multi-day", expand_days=expand_days)
        self.days = days - 1

        if backwards:
            self.start_date -= relativedelta(days=self.days)


    def __iter__(self):
        return self

    
    def peek(self):
        return self.start_date + relativedelta(days=self.days)


    def __next__(self):
        if self.iter_count < self.iterations:
            
            self.end_date    = self.start_date + relativedelta(days=self.days)

            # Collects all important date parameters into a dictionary   
            self.update_date_dict()

            # Increment for next iteration
            self.iter_count += 1    
            self.start_date += relativedelta(days=self.iter_int*(self.days+1))

            return self.date_dict


class FiscalCalendarMonthIterator(BackFillIterator):

    def __init__(self, start_date = TODAY, iterations = 0, backwards = True, logging=True, expand_days=[1,1]):
        super().__init__(start_date, iterations, backwards, logging, iterator_abbrev="date-range", expand_days=expand_days)


    def __iter__(self):
        return self
 

    def peek(self):
        if self.iter_count == 0:
            fiscal_moy = self.start_date
        else:
            fiscal_moy = self.fiscal_moy

        fiscal_start_end = find_fiscal(fiscal_moy.year, fiscal_moy.month)
        start_date     = fiscal_start_end["fis_start"]
    
        # Sets the end date to the last date of the start-date's month. 
        end_date     = start_date + relativedelta(months=1) - datetime.timedelta(days=1)

        return end_date

    def __next__(self):
        if self.iter_count == 0:
            self.fiscal_moy = self.start_date
        fiscal_start_end = find_fiscal(self.fiscal_moy.year, self.fiscal_moy.month)
        
        if self.iter_count < self.iterations:
            end_date     = fiscal_start_end["fis_end"]
            self.end_date = (end_date + relativedelta(months=1)).replace(day=1) - datetime.timedelta(days=1)
            self.start_date   = fiscal_start_end["fis_start"]
            
            # Collects all important date parameters into a dictionary   
            #   Set additional params according to the Fiscal Month and not the Start Date as is the default
            self.update_date_dict(additional_params={"year_month": self.fiscal_moy.strftime("%Y-%m"), "month_name": self.fiscal_moy.strftime("%B")})

            # Increment for next iteration
            self.iter_count += 1    
            
            self.fiscal_moy += relativedelta(months=self.iter_int) #iter_int is either -1 or 1 depending on "backwards"/ direction

            return self.date_dict
