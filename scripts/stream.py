from etl.serp import SerpEtl

if __name__ == "__main__":
    elt = SerpEtl('/Users/tukrre/Downloads/stat_data/')
    elt.start()