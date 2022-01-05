import os

def convert(path, outpath):
    tablename = path.split(os.sep)[-1].split('.')[0].upper()
    print(path, tablename, outpath)

    # get table column types
    types = []
    with open(os.path.join('data', 'create.sql'), encoding='utf-8') as f:
        flag = False
        for line in f:
            if line.startswith(f'CREATE TABLE {tablename}'):
                flag = True
                continue
            if flag:
                w = line.strip().split()[1]
                if w.startswith('INT'):
                    types.append('INT')
                elif w.startswith('FLOAT'):
                    types.append('FLOAT')
                elif w.startswith('VARCHAR'):
                    types.append('VARCHAR')
                else:
                    break

    print(types)

    with open(path, encoding='utf-8') as f:
        with open(outpath, 'w', encoding='utf-8') as outf:
            for line in f:
                values = line.strip().split(',')
                values = [value if types[i] != 'VARCHAR' else f"'{value}'" for i, value in enumerate(values) ]
                sql = f'INSERT INTO {tablename} VALUES ({",".join(values)});'
                outf.write(sql + '\n')


if __name__ == '__main__':
    names = ['part.csv', 'region.csv', 'nation.csv', 'supplier.csv', 'customer.csv', 'partsupp.csv', 'orders.csv', 'lineitem.csv']
    print(names)
    read_file_path = os.path.join('data', 'load.txt')
    with open(read_file_path, 'w', encoding='utf-8') as outf:
        with open(os.path.join('data', 'create.sql'), encoding='utf-8') as f:
            for line in f:
                outf.write(line.strip() + '\n')
        for name in names:
            if name.endswith('.csv'):
                path = os.path.join('data', name)
                outname = name[:-3] + 'txt'
                outpath = os.path.join('data', outname)
                convert(path, outpath)
                sql = f'read {outpath};'
                outf.write(sql + '\n')
