"""
Main dispatch code
"""

from collections import Counter
import socket
import leip
from mad3 import ui
from mad3 import madfile

# add communication functions
ui.init_app(leip.app)

# bulk operation
leip.app.bulk_init = madfile.bulk_init
leip.app.bulk_execute = madfile.bulk_execute


app = leip.app(name='mad3')
app.counter = Counter()

app.discover(globals())

# add a hook for bulk operations


# ensure hostname is defined
if not 'hostname' in app.conf:
    app.warning("Hostname is not defined")
    app.message("Try: m3 conf set hostname '{hostname}'",
                hostname = socket.gethostname())

def dispatch():
    """
    Run the mad3 app
    """
    app.run()
