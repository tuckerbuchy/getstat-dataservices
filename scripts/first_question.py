from analytics.functions import most_top_tens

if __name__ == "__main__":
    most_top_tens(start_date="2017-07-01", end_date="2017-07-31").show(10)