==============
CIA's Factbook
==============

The factbook plugin liberates CIA's Factbook data from CIA web servers.

The CIA's factbook is a valuable source of information about all countries around the World.
CIA is kind (?) enough to share this data openly, but unfortunately, the complete Factbook is
distributed as a 1.7GB zip file of HTML mess and not-so-useful media files.

This package organizes this dataset to be usable from Python. No webscrapping, no quick and
dirt HTML parsing, we done all of this for you. Just `import mundi.plugins.factbook` and be
ready to go ;-)

This module is currently in the main Mundi repository, but in the future it may be
distributed separately.

Usage
=====

>>> import mundi
>>> import mundi.enable_plugin('factbook')
>>> br = mundi.country("BR")
>>> br.factbook.summary()
...
