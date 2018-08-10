from analytics.functions import consistency_across_devices

if __name__ == "__main__":
    consistency_across_devices(start_date="2017-07-01", end_date="2017-07-31").show(10)