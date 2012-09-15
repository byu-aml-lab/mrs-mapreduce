target = halton.so

all: $(target)

$(target): halton.c
	gcc -shared -Wl,-soname,$@ -o $@ -fPIC $< -O2

clean:
	rm $(target)
