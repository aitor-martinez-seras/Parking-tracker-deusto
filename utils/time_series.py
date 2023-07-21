from datetime import datetime, timedelta

from pandas import Series


def check_monotonic_increasing_dates(time_of_time_series: Series, t_initial: datetime):
    """
    Returns True if time series is increasing monotonically and False in case it is not.

	Parameters:
		time_of_time_series (Series): the time part of the time series
		t_initial (datetime): time for the first iteration (should be less than 
            the expected first value)

	Returns:
		increasing (bool): True if time series increases monotonically
    """
    reference_timedelta = timedelta(seconds=0)
    t_before = t_initial
    increasing = True
    for t in time_of_time_series:
        
        # This is in case some number does not have microseconds
        try:
            t = datetime.strptime(t, '%H:%M:%S.%f')
        except ValueError:
            t = datetime.strptime(t, '%H:%M:%S')

        # Check if the diff is negative
        if t - t_before <= reference_timedelta:
            increasing = False
            return increasing
        
        # Update t_before value
        t_before = t

    return increasing


def check_all_rows_are_from_same_date(date_of_time_series: Series, searched_date: str):
    """
    Returns True if all dates from the Series is the same as the searched date
    and False in case it is not.

	Parameters:
		date_of_time_series (Series): the time part of the time series
		searched_date (string): date in the format "YYYY-MM-DD"

	Returns:
		correct_date (bool): True if all dates are from searched date
    """
    correct_date = True
    for d in date_of_time_series:
        if d != searched_date:
            correct_date = False
            return correct_date
    
    return correct_date