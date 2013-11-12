'''
Created on 11 нояб. 2013 г.

@author: Scondo
'''
import logging
try:
    import streetmangler
    locale = streetmangler.Locale('ru_RU')
    db = streetmangler.Database(locale)
    db.Load("data/ru_RU.txt")
    logging.info("Mangle OK")
except BaseException as e:
    db = None
    logging.warn(e.message)
    logging.warn("Mangle Broken")


def unslash(basename):
    '''Replace slashes with parentheses
    '''
    if basename.count('/') == 2:
        basename = basename.replace('/', '(', 1)
        basename = basename.replace('/', ')', 1)
    return basename


def nice(basename, shortname, fullname, place=False):
    basename = unslash(basename)
    if not basename.endswith((fullname, shortname + ".")) and\
       not basename.startswith((fullname, shortname + ".")):
        basename_ = u" ".join((fullname, basename))
    else:
        basename_ = basename
    if db is not None and not place:
        ma = db.CheckCanonicalForm(basename_)
        if ma:
            basename_ = ma[0]
    return (basename_, basename)
