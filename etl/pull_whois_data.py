import pandas as pd
import concurrent.futures
import whois, pathlib, argparse
from urllib. parse import urlparse

#PATHS
parser = argparse.ArgumentParser()
parser.add_argument("input_dir", help="Directory containing urls.parquet data")
parser.add_argument("output_file", help="Output file name")
args = parser.parse_args()
input_dir = args.input_dir
output_file = args.output_file
path = pathlib.Path(input_dir, "urls.parquet")

#USE PANDAS TO READ DATA
df = pd.read_parquet(path)

#AND MAKE A LIST OF TASKS
urls = df['url']
urls = urls.array

#DEFINE FUNCTION TO SUBMIT TASKS FOR NETWORK AND I/O OPERATIONS
def get_whois_data(url):
    
    #WHOIS PULL
    try:
        domain = urlparse(url).netloc
        w = whois.whois(domain)
    except:
        is_registered = 0
        months = -1
        return url, is_registered, months
    
    #CHECK REGISTRATION
    try:
        if (bool(w.domain_name)): is_registered = 1
        else: is_registered = 0     
    except Exception:
        is_registered = 0
    
    #CHECK EXPIRATION INFO
    try:
        #PULL EXPIRATION AND CREATION DATE
        expires = w.expiration_date
        created = w.creation_date
        
        #CHECK IF LIST AND ADAPT
        if (type(expires) == list): expires = expires[0]
        if (type(created) == list): created = created[0]
        
        #TIME DIFFERENCE
        delta = expires - created
        if delta.days < 0: months = -1
        elif delta.days < 30: months =  1
        else: months = int(delta.days/30)
            
    except Exception:
        months = -1
        
    return url, is_registered, months

#CREATE CSV FILE TO PUT DATA IN
f = open(output_file, "w+")
stringWriter = "url,is_registered,months_til_expiration\n"
f.write(stringWriter)
f.close()

#MULTITHREAD NETWORK AND FILE I/O OPERATIONS
with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
    # Start the load operations and mark each future with its URL
    future_to_url = {executor.submit(get_whois_data, url): url for url in urls}
    for future in concurrent.futures.as_compeleted(future_to_url):
        url = future_to_url[future]
        try:
            u, is_registered, months = future.result()
        except Exception as exc:
            print('%r generated an exception: %s' % (url, exc))
        else:
            stringWriter = u + "," + str(is_registered) + "," + str(months) + "\n"
            f = open(output_file, "a")
            f.write(stringWriter)
            f.close()