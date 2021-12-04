from fakedb.managesystem import SystemManager, SystemVisitor


def main():
    system_visitor = SystemVisitor()
    system_manager = SystemManager(system_visitor)
    sql = ''
    while True:
        ipt = input('FakeDB> ')
        if ipt == 'exit' or ipt == 'exit;':
            system_manager.shutdown()
            break
        else:
            if ipt.endswith(';'):
                sql += ' ' + ipt
                ret = system_manager.execute(sql)
                sql = ''
            else:
                sql += ' ' + ipt


if __name__ == '__main__':
    main()