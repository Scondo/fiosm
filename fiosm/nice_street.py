# -*- coding: UTF-8 -*-
'''
Created on 11 нояб. 2013 г.

@author: Scondo
'''
import logging
from itertools import chain
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

synonyms = ((u'Первой Маёвки', u'Первой Маевки', u'1 Маевки'),
            (u'Новый Берингов', u'Н. Берингов', u'Берингов Н.')
            )
all_synonyms = set(chain(*synonyms))


def check_synonym(name):
    if name in all_synonyms:
        for group in synonyms:
            if name in group:
                return group
    else:
        return (name,)


def unslash(basename):
    '''Replace slashes with parentheses
    '''
    if basename.count('/') == 2:
        basename = basename.replace('/', '(', 1)
        basename = basename.replace('/', ')', 1)
    return basename


def nice(basename, shortname, fullname, place=False):
    basename = unslash(basename)
    # Few predefined states (supp. area)
    if fullname == u'город':
        yield u" ".join((fullname, basename))
    elif fullname == u'область':
        yield u" ".join((basename, fullname))
    elif fullname == u'край':
        yield u" ".join((basename, fullname))
    elif fullname == u'чувашия':  # Stupid, but real
        yield u" ".join((basename, u'Чувашия'))

    if basename.startswith('1'):
        pass
    for name in check_synonym(basename):
        # Check when state part already in name
        if not name.endswith((fullname, shortname + ".")) and\
          not name.startswith((fullname, shortname + ".")):
            basename_ = u" ".join((fullname, name))
        else:
            basename_ = name
        if db is not None and not place:
            ma = db.CheckCanonicalForm(basename_)
            if ma:
                basename_ = ma[0]
        yield basename_
