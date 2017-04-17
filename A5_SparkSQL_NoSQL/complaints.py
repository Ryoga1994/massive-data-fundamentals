
# Downloads the dataset into a Python string.




if __name__ == "__main__":
    import requests
    import json
    import datetime

    url = "http://data.consumerfinance.gov/api/views/7zpz-7ury/rows.json"
    response = requests.get(url).content.decode()

    # Uses the json package to turn the JSON string into Python dictionary. ## stupid

    data = json.loads(response)

    # Compute the number of compaints for each year.
    # import pandas as pd
    # df = pd.DataFrame(data['data'])
    #
    # df.head(2)

    #
    # data['data'][0][15]
    #
    # data['meta']
    #
    # data['meta']['view']['columns'][15]

    # 1. count regarding 'The date the CFPB received the complaint' - column 8

    lis = []

    for ele in data['data']:
        lis.append(datetime.datetime.strptime(ele[8],"%Y-%m-%dT%H:%M:%S").year)

    counter = [(y,lis.count(y)) for y in sorted(set(lis))]

    # print out and write to file
    with open('complaints.txt','w') as f:

        for ele in counter:
            f.write("%d %d\n"%(ele[0],ele[1]))
            print("%d %d"%(ele[0],ele[1]))

    # 2. count regarding 'The date the CFPB sent the complaint to the company' column 21

    # lis = []
    #
    # for ele in data['data']:
    #     lis.append(datetime.datetime.strptime(ele[21],"%Y-%m-%dT%H:%M:%S").year)
    #
    # counter = [(y,lis.count(y)) for y in sorted(set(lis))]

    # print out
    # for ele in counter:
    #     print("%d %d"%(ele[0],ele[1]))

    # Extra Credit Option #1: (0.25 point):
    # How many complaints were there against PayPal Holdings, Inc?
    #  Put your answer in paypal.txt
    res = [ele[15] == 'PayPal Holdings, Inc.' for ele in data['data']]
    with open('paypal.txt','w') as f:
        f.write('%d '%(sum(res)))
        print('%d '%(sum(res)))


