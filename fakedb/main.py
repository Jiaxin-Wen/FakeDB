from managesystem.system_manager import SystemManager


def main():
    system_manager = SystemManager()
    while True:
        ipt = input('FakeDB> ')
        if ipt == 'exit':
            break
        ret = system_manager.execute(ipt)
        print(ret)

if __name__ == '__main__':
    main()