from fakedb.managesystem import SystemManager, SystemVisitor


def main():
    system_visitor = SystemVisitor()
    system_manager = SystemManager(system_visitor)
    sql = ''

    def read_file(filepath):
        with open(filepath, encoding='utf-8') as f:
            sql = ''
            for line in f:
                # print(filepath, line)
                line = line.strip()
                if not line:
                    continue
                if line.startswith('read'):
                    try:
                        _filepath = line.strip()[:-1].split()[1]
                        read_file(_filepath)
                    except Exception as e:
                        print(e)
                    continue
                if line.endswith(';'):
                    sql += ' ' + line
                    # print(f'execute sql:{sql}')
                    ret = system_manager.execute(sql)
                    sql = ''
                else:
                    sql += ' ' + line

    while True:
        ipt = input('FakeDB> ')
        if ipt == 'exit' or ipt == 'exit;':
            system_manager.shutdown()
            break
        elif ipt.startswith('read'):
            try:
                filepath = ipt.strip()[:-1].split()[1]
                read_file(filepath)
            except Exception as e:
                print(e)
        else:
            if ipt.endswith(';'):
                sql += ' ' + ipt
                ret = system_manager.execute(sql)
                sql = ''
            else:
                sql += ' ' + ipt


if __name__ == '__main__':
    main()