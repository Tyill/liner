CPP             = g++
RM              = rm -f
CPPFLAGS        = -Wall -O2 -std=c++17 -g -Wno-write-strings
LDFLAGS         = -lzmq
SOURCES         = $(wildcard *.cpp)
TARGETS         = $(SOURCES:%.cpp=%)

all: ${TARGETS}

clean:
	${RM} *.obj *~* ${TARGETS}

${TARGETS}:
	${CPP} ${CPPFLAGS} -o $@ ${@:%=%.cpp} ${LDFLAGS}