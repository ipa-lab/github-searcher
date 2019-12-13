# This script implements a tool that exhaustively samples GitHub Code Search
# results. It is written in a semi-literal style: it should be possible to
# read through the source in a linear fashion, more or less. Enjoy.

import os, sys, argparse, shutil, time, signal
import base64, sqlite3, csv
import requests

# Before we get to the fun stuff, we need to parse and validate arguments,
# check environemtn variables, set up the help text and so on.

# fix for argparse: ensure terminal width is determined correctly
os.environ['COLUMNS'] = str(shutil.get_terminal_size().columns)

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description='Exhaustively sample the GitHub Code Search API.', 
    epilog='') # TODO

parser.add_argument('query', metavar='QUERY', help='search query')

parser.add_argument('--database', metavar='FILE', default='results.db', 
    help='search results database file (default: results.db)')

parser.add_argument('--statistics', metavar='FILE', default='sampling.csv', 
    help='sampling statistics file (default: sampling.csv)')

parser.add_argument('--min-size', metavar='BYTES', type=int, default=1, 
    help='minimum code file size (default: 1)')

# Only files smaller than 384 KB are searchable via the GitHub API.
MAX_FILE_SIZE = 393216

parser.add_argument('--max-size', metavar='BYTES', type=int, 
    default=MAX_FILE_SIZE, 
    help=f'maximum code file size (default: {MAX_FILE_SIZE})')

parser.add_argument('--stratum-size', metavar='BYTES', type=int, default=1,
    help='''length of file size ranges into which population is partitioned 
    (default: 1)''')

parser.add_argument('--no-throttle', dest='throttle', action='store_false', 
    help='disable request throttling')

parser.add_argument('--github-token', metavar='TOKEN', 
    default=os.environ.get('GITHUB_TOKEN'), 
    help='''personal access token for GitHub 
    (by default, the environment variable GITHUB_TOKEN is used)''')

args = parser.parse_args()

if args.min_size < 1:
    sys.exit('min-size must be positive')
if args.min_size >= args.max_size:
    sys.exit('min-size must be less than or equal to max-size')
if args.max_size < 1:
    sys.exit('max-size must be positive')
if args.max_size > MAX_FILE_SIZE:
    sys.exit(f'max-size must be less than or equal to {MAX_FILE_SIZE}')
if args.stratum_size < 1:
    sys.exit('stratum-size must be positive')
if not args.github_token:
    sys.exit('missing environment variable GITHUB_TOKEN')

#-----------------------------------------------------------------------------

# The GitHub Code Search API is limited to 1000 results per query. To get
# around this limitation, we can take advantage of the ability to restrict
# searches to files of a certain size. By repeatedly searching with the same
# query but different size ranges, we can reach a pretty good sample of the
# overall population. This is a technique known as *stratified sampling*. The
# strata in our case are non-overlapping file size ranges.

# Let's start with some global definitions. We need to keep track of the first
# and last size in the current stratum...
strat_first = args.min_size
strat_last = min(args.min_size + args.stratum_size - 1, args.max_size)

# ...as well as the current stratum's population and the amount of files
# sampled so far (in the current stratum). A value of -1 indicates "unknown".
pop = -1
sam = -1

# We also have an estimate of the overall population, and we keep track of the
# total sample size so far.
est_pop = -1
total_sam = -1

#-----------------------------------------------------------------------------

# During our search we want to display a table of all the strata sampled so
# far, plus the stratum currently being sampled, some summary information, and
# a status message. These last three items will be continuously updated to
# signal the progress that's being made.

# First, let's just print the table header:
print('                 ┌────────────┬────────────┐')
print('                 │ population │   sample   │')
print('                 ├────────────┼────────────┤')

# Now we'll define some functions to display the current progress. The status
# message is kept in a global variable, because it'll make error handling a
# bit easier later on.

status_msg = ''

# This function prints all the current information.
def print_progress():
    n       = strat_first
    m       = strat_last
    size    = '%d' % n if n == m else '%d .. %d' % (n,m)
    pop_str = str(pop) if pop > -1 else ''
    sam_str = str(sam) if sam > -1 else ''
    per     = '%6.2f%%' % (sam/pop*100) if pop > 0 else ''
    print('%16s │ %10s │ %10s │ %6s' % (size, pop_str, sam_str, per))

    print('                 ├────────────┼────────────┤')
    print('                 │ population │   sample   │')
    print('                 └────────────┴────────────┘')

    n       = args.min_size
    m       = args.max_size
    size    = '%d' % n if n == m else '%d .. %d' % (n,m)
    pop_str = str(est_pop) if est_pop > -1 else ''
    sam_str = str(total_sam) if total_sam > -1 else ''
    per     = '%6.2f%%' % (total_sam/est_pop*100) if est_pop > 0 else ''
    print('%16s   %10s   %10s   %6s' % (size, pop_str, sam_str, per))
    print('                   (estimated)') if est_pop > -1 else print()

    print()
    print(status_msg)

# This one does the same, except it overwrites what was previously printed.
# Additionally, there is an option to leave the current stratum standing,
# allowing us to add new lines to the table.
def overwrite_progress(leave_current_stratum=False):
    num_lines = 7 if leave_current_stratum else 8
    sys.stdout.write(f'\033[{num_lines}F\r\033[J') # ANSI code to clear lines
    print_progress()

# This is a convenient function for just updating the status message. It also
# returns the old message, so it can be restored later if desired.
def update_status(msg):
    global status_msg
    old_msg = status_msg
    status_msg = msg
    sys.stdout.write('\033[F\r\033[J')
    print(status_msg)
    return old_msg

#-----------------------------------------------------------------------------

# To access the the GitHub API, we define a little helper function that makes
# an authorized GET request and throttles the number of requests per second so
# as not to run afoul of GitHub's rate limiting. Should a rate limiting error
# occur nonetheless, the function waits the appropiate amount of time before
# automatically retrying the request.

def get(url, params={}):
    if args.throttle:
        time.sleep(0.72) # throttle requests to ~5000 per hour
    res = requests.get(url, params, headers=
        {'Authorization': f'token {args.github_token}'})
    if res.status_code == 403:
        return handle_rate_limit_error(res)
    else:
        return res

def handle_rate_limit_error(res):
    t = res.headers.get('X-RateLimit-Reset')
    if t is not None: 
        t = int(int(t) - time.time())
    else: 
        t = int(res.headers.get('Retry-After', 60))
    err_msg = f'Exceeded rate limit. Retrying after {t} seconds...'
    old_msg = update_status(err_msg)
    time.sleep(t)
    update_status(old_msg)
    return get(res.url)

# We also define a convenient function to do the code search for a specific
# stratum. Note that we sort the search results by how recently a file has
# been indexed by GitHub.

def search(a,b,order='asc'):
    return get('https://api.github.com/search/code',
               params={'q': f'{args.query} size:{a}..{b}', 
                'sort': 'indexed', 'order': order, 'per_page': 100})

# To download all files returned by a code search (up to the limit of 1000
# imposed by GitHub), we need to deal with pagination. On each page, we
# download all available files and add them and their metadata to our results
# database (which will be set up in the next section).

def download_all_files(res):
    download_files_from_page(res)
    while 'next' in res.links:
        update_status('Getting next page of search results...')
        res = get(res.links['next']['url'])
        download_files_from_page(res)
    update_status('')

def download_files_from_page(res):
    global sam, total_sam
    update_status('Downloading files...')
    for item in res.json()['items']:
        repo = item['repository']
        insert_repo(repo)
        file = get(item['url']).json()
        insert_file(file, repo['id'])
        sam += 1
        total_sam += 1
        overwrite_progress()

#-----------------------------------------------------------------------------

# This is a good place to open the connection to the results database, or
# create one if it doesn't exist yet. The database schema follows the GitHub
# API response schema. Our 'insert_repo' and 'insert_file' functions directly
# take a JSON response dictionary.

db = sqlite3.connect(args.database)
db.executescript('''
    CREATE TABLE IF NOT EXISTS repo 
    ( repo_id INTEGER PRIMARY KEY
    , name TEXT NOT NULL
    , full_name TEXT NOT NULL
    , description TEXT
    , url TEXT NOT NULL
    , fork INTEGER NOT NULL
    , owner_id INTEGER NOT NULL
    , owner_login TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS file
    ( file_id INTEGER PRIMARY KEY
    , name TEXT NOT NULL
    , path TEXT NOT NULL
    , size INTEGER NOT NULL
    , sha TEXT NOT NULL
    , content TEXT NOT NULL
    , repo_id INTEGER NOT NULL
    , FOREIGN KEY (repo_id) REFERENCES repo(repo_id)
    , UNIQUE(path,repo_id)
    );
    ''')

def insert_repo(repo):
    db.execute('''
        INSERT OR IGNORE INTO repo 
            ( repo_id, name, full_name, description, url, fork
            , owner_id, owner_login
            )
        VALUES (?,?,?,?,?,?,?,?)
        ''',
        ( repo['id']
        , repo['name']
        , repo['full_name']
        , repo['description']
        , repo['url']
        , int(repo['fork'])
        , repo['owner']['id']
        , repo['owner']['login']
        ))

def insert_file(file,repo_id):
    db.execute('''
        INSERT OR IGNORE INTO file
            (name, path, size, sha, content, repo_id)
        VALUES (?,?,?,?,?,?)
        ''',
        ( file['name']
        , file['path']
        , file['size']
        , file['sha']
        , base64.b64decode(file['content']).decode('UTF-8')
        , repo_id
        ))

#-----------------------------------------------------------------------------

# Now we can finally get into it! 

# First, let's get an estimate of the total population. 
# Note that this is a very, very unstable number that can not be relied upon!

status_msg = 'Getting an estimate of the overall population...'
print_progress()

res = search(args.min_size, args.max_size)
est_pop = int(res.json()['total_count'])
total_sam = 0

# Before starting the iterative search process, let's see if we have a
# sampling statistics file that we could use to continue a previous search. If
# so, let's get our data structures and UI up-to-date; otherwise, create a new
# statistics file.

if os.path.isfile(args.statistics):
    with open(args.statistics, 'r') as f:
        fr = csv.reader(f)
        next(fr) # skip header
        for row in fr:
            strat_first = int(row[0])
            strat_last = int(row[1])
            pop = int(row[2])
            sam = int(row[3])
            total_sam += sam
            overwrite_progress()
            overwrite_progress(leave_current_stratum=True)
        strat_first += args.stratum_size
        strat_last = min(strat_last + args.stratum_size, args.max_size)
        pop = -1
        sam = -1
else:
    with open(args.statistics, 'w') as f:
        f.write('stratum_first,stratum_last,population,sample\n')

statsfile = open(args.statistics, 'a', newline='')
stats = csv.writer(statsfile)

#-----------------------------------------------------------------------------

# This is a good place to define a signal handler to cleanly deal with Ctrl-C.
# If the user quits the program and cancels the search, we want to allow him
# to later continue more-or-less where he left of. Thus we need to properly
# close the database and statistic file.

def signal_handler(sig,frame):
    db.close()
    statsfile.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

#-----------------------------------------------------------------------------

# Iterating through all the strata, we sample as much as we can.
while strat_first <= args.max_size:
    status_msg = 'Searching...'
    overwrite_progress()
    res = search(strat_first, strat_last)
    pop = int(res.json()['total_count'])
    sam = 0
    overwrite_progress()

    download_all_files(res)

    # To stretch the 1000-results-per-query limit, we can simply repeat the
    # search with the sort order reversed, thus sampling the stratum population
    # from both ends, so to speak. This gives us a maximum sample size of 2000
    # per stratum.
    if pop > 1000:
        update_status('Repeating search with reverse sort order...')
        res = search(strat_first, strat_last, order='desc')
        
        # Due to the instability of search results, we might get a different
        # population count on the second query. We will take the maximum of
        # the two population counts for this stratum as a conservative
        # estimate.
        pop2 = int(res.json()['total_count'])
        pop = max(pop,pop2)
        overwrite_progress()

        download_all_files(res)

    # Add a new line to the table...
    
    overwrite_progress(leave_current_stratum=True)
    stats.writerow([strat_first,strat_last,pop,sam])

    # ...and move on to the next stratum.
    
    strat_first += args.stratum_size
    strat_last = min(strat_last + args.stratum_size, args.max_size)
    pop = -1
    sam = -1
