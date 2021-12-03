from fakedb.managesystem import SystemManager, SystemVisitor


def main():
    system_visitor = SystemVisitor()
    system_manager = SystemManager(system_visitor)
    while True:
        ipt = input('FakeDB> ')
        if ipt == 'exit':
            system_manager.shutdown()
            break
        ret = system_manager.execute(ipt)

if __name__ == '__main__':
    main()