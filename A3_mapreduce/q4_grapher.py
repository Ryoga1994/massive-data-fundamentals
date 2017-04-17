
import matplotlib.pyplot as plt
import sys
import re

if __name__=="__main__":

    hours = []
    stdevs = []
    with open("q4_hop23.txt") as f:
        lines = f.readlines()

        for line in lines:
            hour, stdev = line.split('\t')

            hours.append(hour)
            stdevs.append(stdev)


    plt.plot(hours,stdevs)
    plt.title("Time Analysis by hour")
    plt.xlabel('hour')
    plt.ylabel('standard deviation of response time')

    # This would display locally:
    # plt.show()
    # This will save the figure as a png and as a PDF:
    plt.savefig("q4_graph.png")
    plt.savefig("q4_graph.pdf")
