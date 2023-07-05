from datetime import datetime, timedelta

from pandas import Series


def check_monotonic_increasing_dates(time_series: Series, t_initial: datetime):
    """
    Returns True if time series is increasing monotonically and False in case it is not.

	Parameters:
		time_series (Series): A decimal integer
		t_initial (datetime): Another decimal integer

	Returns:
		increasing (bool): True if time series increases monotonically
    """
    reference_timedelta = timedelta(seconds=0)
    t_before = t_initial
    for t in time_series:
        
        # This is in case some number does not have microseconds
        try:
            t = datetime.strptime(t, '%H:%M:%S.%f')
        except ValueError:
            t = datetime.strptime(t, '%H:%M:%S')

        # Check if the diff is negative
        if t - t_before <= reference_timedelta:
            return False
        
        # Update t_before value
        t_before = t

    return True