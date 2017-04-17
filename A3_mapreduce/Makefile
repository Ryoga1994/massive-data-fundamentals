#
# Parameters
export ASSIGNMENT=3
export REQUIRED_FILES=q1_ipaddresses.py q1_hostnames.py q1_hostnames.py q1_hostnames.txt \
	q2_multiple.py q2_multiple.txt\
        q3_links.py q3_linksdev.txt \
        q4_hop23.py q4_hop23.txt 
export OPTIONAL_FILES=q4_grapher.py q4_graph.png

#
#
# Figure out which python we can use
PYTHON3=$(shell which python35 || which python3.5 || which python34 || which python3.4 || echo python3)

check:
	@$(PYTHON3) -c "print('Python3 is operational');"
	@$(PYTHON3) validator.py --check

submit:
	$(PYTHON3) validator.py --zip --check


clean:
	/bin/rm -f *~
