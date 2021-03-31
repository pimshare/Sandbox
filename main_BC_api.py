import requests


# this is where I can test the Authentication --> so far I am gettign an 401 response, shich is not cool


def main():
    r = requests.get("https://api.businesscentral.dynamics.com/v1.0/cronus.com/sandbox2/api/v1.0",
                     auth=('max.rudat@share.eu', 'Welcome2021!'))
    r.status_code


if __name__ == '__main__':
    main()
