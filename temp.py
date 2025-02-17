
def extract(url, table_attribs):

    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[1].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if col[0].find('a') is not None:
            data_dict= {"Name": col[0].a.contents[0],
                        "MC_USD_Billion": col[2].a.contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)

    return df

print(df)



def extract(url, table_attribs):

    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[1].find_all('tr')
 # ✅ Fix: Ensure tables[1] exists before accessing it
    if len(tables) < 2:
        print("Error: Table index out of range. Returning empty DataFrame.")
        return df  

    # rows = tables[1].find_all('tr')

    for row in rows:
        col = row.find_all('td')

        # ✅ Fix: Ensure col has at least 3 elements before accessing col[0] or col[2]
        if len(col) < 3:
            continue  # Skip rows with missing columns

        name = col[0].a.text.strip() if col[0].find('a') else col[0].text.strip()
        mc_usd_billion = col[2].text.strip()  # ✅ Extract Market Cap correctly

        data_dict = {"Name": name, "MC_USD_Billion": mc_usd_billion}
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)

    return df

df = extract(url, table_attribs)
print(df)  

