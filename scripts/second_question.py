from analytics.functions import most_rank_1_changes

if __name__ == "__main__":
    most_rank_1_changes(start_date="2017-07-01", end_date="2017-07-31").show(10)