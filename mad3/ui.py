

import colors
import leip

def logo(self):
    """
    Return Logo string.
    
    """

    return colors.color('m3', bg=74) + ' '


def warning(self, *args, **kwargs):
    """Print a warning to screen."""
    fg = self.conf['color']['warning']['fg']
    bg = self.conf['color']['warning']['bg']
    message = ' '.join(map(str, args))
    if kwargs:
        message = message.format(**kwargs)
    print(logo(self) + colors.color(message, bg=bg, fg=fg))


def message(self, *args, **kwargs):
    """Print a message to screen."""
    message = ' '.join(map(str, args))
    if kwargs:
        message = message.format(**kwargs)
    print(logo(self) + message)


def init_app(appclass: leip.app):
    """Initialization attach functions to the app class."""
    appclass.logo = logo
    appclass.warning = warning
    appclass.message = message
