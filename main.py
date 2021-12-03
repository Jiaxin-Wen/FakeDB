from fakedb.managesystem import SystemManager, SystemVisitor


def main():
    system_visitor = SystemVisitor()
    system_manager = SystemManager(system_visitor)
    while True:
        ipt = input('FakeDB> ')
        if ipt == 'exit':
            break
        ret = system_manager.execute(ipt)
        print(ret)

if __name__ == '__main__':
    main()