from __future__ import division
import ctypes

path = '/aml/home/jlund3/workspace/mrs-mapreduce/examples/pi/halton.so'
halton_dll = ctypes.CDLL(path)
pi = halton_dll.pi
pi.restype = ctypes.c_longlong
pi.argtypes = (ctypes.c_longlong, ctypes.c_longlong)

size = 100000000
inside = pi(10003, size)
print 4 * inside / size
