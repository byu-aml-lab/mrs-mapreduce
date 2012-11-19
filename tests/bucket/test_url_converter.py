from mrs.bucket import URLConverter

def test_local_to_global():
    c = URLConverter('myhost', 42, '/my/path')

    url = c.local_to_global('/my/path/xyz.txt')
    assert url == 'http://myhost:42/xyz.txt'

def test_local_to_global_outside_dir():
    c = URLConverter('myhost', 42, '/my/path')

    url = c.local_to_global('/other/path/xyz.txt')
    assert url == '/other/path/xyz.txt'

def test_global_to_local():
    c = URLConverter('myhost', 42, '/my/path')
    master = 'server:8080'

    url = c.global_to_local('http://myhost:42/xyz.txt', master)
    assert url == '/my/path/xyz.txt'

def test_global_to_local_other():
    c = URLConverter('myhost', 42, '/my/path')
    master = 'server:8080'

    url = c.global_to_local('http://other:443/xyz.txt', master)
    assert url == 'http://other:443/xyz.txt'

def test_global_to_local_master():
    c = URLConverter('myhost', 42, '/my/path')
    master = 'server:8080'

    url = c.global_to_local('http:///xyz.txt', master)
    assert url == 'http://server:8080/xyz.txt'

# vim: et sw=4 sts=4
