bin = halton

all: $(bin)

$(bin): halton.c
	gcc halton.c -o $(bin)

clean:
	rm $(bin)

run: $(bin)
	@./$(bin)
