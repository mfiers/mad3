


import colors

def logo(self):
    return colors.color('m3', bg=74) + colors.color(' ', bg=202)

def warning(self, *args, **kwargs):
    fg = self.conf['color']['warning']['fg']
    bg = self.conf['color']['warning']['bg']
    message = " ".join(args)
    if kwargs:
        message = message.format(**kwargs)
    print(logo(self) + colors.color(message, bg=bg, fg=fg))

def message(self, *args, **kwargs):
    fg = self.conf['color']['message']['fg']
    bg = self.conf['color']['message']['bg']
    message = " ".join(args)
    if kwargs:
        message = message.format(**kwargs)
    print(logo(self) + colors.color(message, bg=bg, fg=fg))

def init_app(app):
    app.logo = logo
    app.warning = warning
    app.message = message
