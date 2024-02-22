## HQL Query
- HQLQuery class contains a HQL File reference to files located in the job's "/hql" file. 
- Class contains a run method that uses a Spark session to run the query.

```python
sample1 = HQLQuery("sample1")

sample1.run(run_settings)
```

- You can indicate you'd like an HQL Query run as a temp table using the `is_temp` parameter in the HQL Query class. 

```python
HQLQuery("sample1", is_temp=True).run(run_settings)
```

- The table will be saved to the EMR's attached hdfs temporarily granting your session access to the table. 

## HQL Runner
- HQL Runners are used to run multiple files in sequence or to backfill. 

```python
sample2 = HQLQuery("sample2")

HQLRunner([sample1, sample2]).run(run_settings)

##To save all tables in the runner temporarily you can indicate in the Runner or specify individually
HQLRunner([sample1, sample2], is_temp=True).run(run_settings)
HQLRunner([HQLQuery("sample1", is_temp=True), HQLQuery("sample2", is_temp=True)]).run(run_settings)

#You can save some tables as temporary ones and others as permanent S3 tables. 
HQLRunner([HQLQuery("sample1", is_temp=True), HQLQuery("sample2")]).run(run_settings)
```

## Iterators
- Iterators are used to backfill 
- There are several types of iterators that mostly use the same parameters

```python
# My birthday is 5/23
bci = BillCycleIterator(start_date = "2020-05-23", iterations = 2, backwards = False)
fmi = FiscalMonthIterator(start_date = "2020-05-23", iterations = 2)
cmi = CalendarMonthIterator(start_date = "2020-05-23", iterations = 2)
```

- You can use multiple iterators in a runner, running them sequentially for each query.

```python
HQLRunner(sample1).run(run_settings, [bci, fmi, cmi])
# Example 1: 3 iterators, 1 file, 2 cycles (2 x 1 x 3) = 6 runs
#   1. BillCycleIterator      | sample1 | May 2020
#   2. FiscalMonthIterator    | sample1 | May 2020
#   3. CalendarMonthIterator  | sample1 | May 2020
#   4. BillCycleIterator      | sample1 | June 2020 **notice the backwards=False flag
#   5. FiscalMonthIterator    | sample1 | April 2020
#   6. CalendarMonthIterator  | sample1 | April 2020
```

- Iterators have some useful parameters. `backwards = True` indicates you'd like for the backfill to run from the start_date to end_date back in time, where as setting it to `False` will begin at the start_date and move forward. 
- `is_complete` is a boolean indicating whether you'd like the iterator to start iterating at the first month that has completed. 

## Runner Iterate Method
- The HQLRunner's `iterate` method allows you to loop through an HQLRunner. The `run` method alone completes all looping/iterations in one go without the ability for the dev to run separate operations in between iterations. One reason this is useful is for separating Daily and Monthly type aggregations. Instead of running a daily aggregation for each month aggregation you can run the daily separate and then all monthly aggregations. 

```python
daily_agg = HQLRunner([HQLQuery("Temp/voice_temp", is_temp=True), HQLQuery("ir_ild_daily")])
monthly_agg = HQLRunner(HQLQuery("mvno_ir_ild"))

fmi = FiscalMonthIterator(start_date=run_settings['load_date'], iterations=run_settings["MONTHS_TO_LOAD"])
cmi = CalendarMonthIterator(start_date=run_settings['load_date'], iterations=run_settings["MONTHS_TO_LOAD"])
fcmi = FiscalCalendarMonthIterator(start_date=run_settings['load_date'], iterations=run_settings["MONTHS_TO_LOAD"])

for i in range(run_settings["MONTHS_TO_LOAD"]):
    daily_agg.iterate(run_settings, fcmi)
    monthly_agg.iterate(run_settings, [fmi, cmi])
```

The example above is taken from the `mobile-ir-ild` job with [MR](https://gitlab.spectrumflow.net/awspilot/mobile-jobs/-/merge_requests/1022#2e0cd4d39cd93d39bb61b89fbcbc2f916dfba7eb).

## Current Jobs in Use
- Speedboost Speed Test Results: MVNO-data-loads/msb-speed-test-results
- Daily Usage Cube: MVNO-Druid/mvno-usage-daily
- Bill Cycle Reporting: analysis-reports/reporting-features
- Account Lines Process: MobileLines/accounts-lines-process
- Account-lines-delta: MobileLines/accounts-lines-delta
- Export CBRS:  MVNO-data-loads/export-cbrs-usage (uses TEMP tables)