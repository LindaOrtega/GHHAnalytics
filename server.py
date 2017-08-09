#!/usr/bin/env python2.7

"""
Columbia's COMS W4111.001 Introduction to Databases
Example Webserver

To run locally:

    python server.py

Go to http://localhost:8111 in your browser.

A debugger such as "pdb" may be helpful for debugging.
Read about it online.
"""

import os
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)


#
# The following is a dummy URI that does not connect to a valid database. You will need to modify it to connect to your Part 2 database in order to use the data.
#
# XXX: The URI should be in the format of:
#
#     postgresql://USER:PASSWORD@w4111a.eastus.cloudapp.azure.com/proj1part2
#
# For example, if you had username gravano and password foobar, then the following line would be:
#
#     DATABASEURI = "postgresql://gravano:foobar@w4111a.eastus.cloudapp.azure.com/proj1part2"
#
"""
DATABASEURI = "postgresql://mm994:8528@w4111vm.eastus.cloudapp.azure.com/w4111"
"""

# This line creates a database engine that knows how to connect to the URI above
"""
engine = create_engine(DATABASEURI)
"""

@app.before_request
def before_request():
  """
  This function is run at the beginning of every web request
  (every time you enter an address in the web browser).
  We use it to setup a database connection that can be used throughout the request.

  The variable g is globally accessible.
  """
  try:
    g.conn = engine.connect()
  except:
    print "uh oh, problem connecting to database"
    import traceback; traceback.print_exc()
    g.conn = None

@app.teardown_request
def teardown_request(exception):
  """
  At the end of the web request, this makes sure to close the database connection.
  If you don't, the database could run out of memory!
  """
  try:
    g.conn.close()
  except Exception as e:
    pass

#
# @app.route is a decorator around index() that means:
#   run index() whenever the user tries to access the "/" path using a GET request
#
# If you wanted the user to go to, for example, localhost:8111/foobar/ with POST or GET then you could use:
#
#       @app.route("/foobar/", methods=["POST", "GET"])
#
# PROTIP: (the trailing / in the path is important)
#
# see for routing: http://flask.pocoo.org/docs/0.10/quickstart/#routing
# see for decorators: http://simeonfranklin.com/blog/2012/jul/1/python-decorators-in-12-steps/
#
@app.route('/')
def index():
  """
  request is a special object that Flask provides to access web request information:

  request.method:   "GET" or "POST"
  request.form:     if the browser submitted a form, this contains the data in the form
  request.args:     dictionary of URL arguments, e.g., {a:1, b:2} for http://localhost?a=1&b=2

  See its API: http://flask.pocoo.org/docs/0.10/api/#incoming-request-data
  """

  queries = {'po_num_mem':'select P.pid as id, count (distinct J.mid) as num from joins_partner J join partner_organization P on J.pid=P.pid group by P.pid, P.pname',
	'ig_num_mem':'select I.gid as id, count (distinct J.mid) as num from joins_group J join interest_group I on J.gid=I.gid group by I.gid, I.gname',
	'po_num_content':'select P.pid as id, count (*) as num from member M, joins_partner J, partner_organization P, content C where M.mid=C.mid and C.mid=J.mid and J.pid=P.pid group by P.pid, P.pname',
	'ig_num_content':'select I.gid as id, count (*) as num from member M, joins_group J, interest_group I, content C where M.mid=C.mid and C.mid=J.mid and J.gid=I.gid group by I.gid, I.gname',
	'po_num_active':'select P.pid as id, count (distinct C.mid) as num from content C, joins_partner J, partner_organization P where C.mid=J.mid and J.pid = P.pid group by P.pid, P.pname',
	'ig_num_active':'select I.gid as id, count (distinct C.mid) as num from content C, joins_group J, interest_group I where C.mid=J.mid and J.gid = I.gid group by I.gid, I.gname'}

  # Gets partner org stats
  pcursor = g.conn.execute("SELECT pid, pname FROM partner_organization")
  partners = []

  po_num_mem = get_num(queries['po_num_mem'])
  po_num_content = get_num(queries['po_num_content'])
  po_num_active = get_num(queries['po_num_active'])

  for p in pcursor:
  	partners.append({'id':p[0],'name':p[1],
		'num_mem':check_if_zero(p[0],po_num_mem),
		'num_content':check_if_zero(p[0],po_num_content),
		'num_active':check_if_zero(p[0],po_num_active)})

  pcursor.close()
  context = dict(partners = partners)

  # Gets interest group stats
  gcursor = g.conn.execute("SELECT gid, gname FROM interest_group")
  groups = []

  ig_num_mem = get_num(queries['ig_num_mem'])
  ig_num_content = get_num(queries['ig_num_content'])
  ig_num_active = get_num(queries['ig_num_active'])

  for ig in gcursor:
    groups.append({'id':ig[0],'name':ig[1],
    	'num_mem':check_if_zero(ig[0],ig_num_mem),
     	'num_content':check_if_zero(ig[0],ig_num_content),
     	'num_active':check_if_zero(ig[0],ig_num_active)})

  gcursor.close()
  context['groups'] = groups

  return render_template("index.html", **context)

# Returns a dict of entity id's with a numerical value
# Takes as input a query string
# that returns two columns labeled 'id' and 'num'
def get_num(query):
  stats = {}
  cursor = g.conn.execute(query)

  for s in cursor:
  	stats[s['id']] = s['num']

  return stats

# Checks if idd is in the number dictionaries
# Returns zero if not, or the value otherwise
def check_if_zero(idd, num_dict):
  if idd not in num_dict:
  	return 0
  else:
  	return num_dict[idd]

#
# This is an example of a different path.  You can see it at:
#
#     localhost:8111/category
#     localhost:8111/coverage
#
# Notice that the function name is category() rather than index()
# The functions for each app.route need to have different names
#

# Get category stats
@app.route('/category')
def category():
  queries = {'num_content':'select catname as id, count (cid) as num from contcat group by catname',
				'num_mem_hit':'select C.catname as id, count (M.cid) as num from contcat C, member_hit M where C.cid=M.cid group by C.catname',
				'num_nonmem_hit':'select C.catname as id, count (N.cid) as num from contcat C, nonmember_hit N where C.cid=N.cid group by C.catname',
				'num_comment':'select Cat.catname as id, count (Com.cid) as num from contcat Cat, comment Com where Cat.cid=Com.cid group by Cat.catname'}

  ccursor = g.conn.execute("select catname from category")
  categories = []

  cat_num_content = get_num(queries['num_content'])
  cat_num_mem_hit = get_num(queries['num_mem_hit'])
  cat_num_nonmem_hit = get_num(queries['num_nonmem_hit'])
  cat_num_comment = get_num(queries['num_comment'])

  idx = 0
  for c in ccursor:
    categories.append({'idx':idx,'name':c[0],
		'num_content':check_if_zero(c[0],cat_num_content),
		'num_mem_hit':check_if_zero(c[0],cat_num_mem_hit),
		'num_nonmem_hit':check_if_zero(c[0],cat_num_nonmem_hit),
		'num_comment':check_if_zero(c[0],cat_num_comment)})
    idx = idx + 1

  ccursor.close()
  context = dict(categories = categories)

  return render_template("category.html", **context)

# coverage path
# Also accepts /coverage?zip=zero
# Also accepts /coverage?sr=zero
# Also accepts /coverage?zip=zero&sr=zero

@app.route('/coverage')
@app.route('/coverage/<string:display>')
def coverage(display = 'all_both',display_all_zip = True, display_all_sr = True):
  queries = {'zip_num_po': 'select Cov.zipcode as id, count (P.pid) as num from covers Cov, partner_organization P where Cov.pid=P.pid group by Cov.zipcode',
  'sr_num_po':'select S.sid as id, count (Par.pid) as num from provides Pro, partner_organization Par, service S where Pro.pid=Par.pid and Pro.sid=S.sid group by S.sid, S.sname'}

  zipcodes = []
  services = []

  if display == 'zero_zip':
    display_all_zip = False

  # Get zip code data
  zcursor = g.conn.execute("select zipcode from zip_code")
  zip_num_po = get_num(queries['zip_num_po'])

  for z in zcursor:
    if not display_all_zip:
      if check_if_zero(z[0], zip_num_po) == 0:
        zipcodes.append({'zipcode':z[0], 'num_po':check_if_zero(z[0], zip_num_po)})

    else:
      zipcodes.append({'zipcode':z[0], 'num_po':check_if_zero(z[0], zip_num_po)})

  zcursor.close()
  context = dict(zipcodes = zipcodes)

  # Get services data
  scursor = g.conn.execute("select sid, sname from service")
  sr_num_po = get_num(queries['sr_num_po'])

  for s in scursor:
    services.append({'id':s[0], 'name':s[1],'num_po':check_if_zero(s[0], sr_num_po)})

  scursor.close()
  context['services'] = services

  return render_template("coverage.html", **context)

@app.route('/login')
def login():
    abort(401)
    this_is_never_executed()


if __name__ == "__main__":
  import click

  @click.command()
  @click.option('--debug', is_flag=True)
  @click.option('--threaded', is_flag=True)
  @click.argument('HOST', default='0.0.0.0')
  @click.argument('PORT', default=8111, type=int)
  def run(debug, threaded, host, port):
    """
    This function handles command line parameters.
    Run the server using:

        python server.py

    Show the help text using:

        python server.py --help

    """

    HOST, PORT = host, port
    print "running on %s:%d" % (HOST, PORT)
    app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)


  run()
