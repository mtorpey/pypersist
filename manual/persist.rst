The ``persist`` decorator
=========================

The most important feature of pypersist is the ``persist`` decorator.
As shown in the :ref:`Examples` section, you can use it by simply writing
``@persist`` above any function you wish to memoise.

``persist`` can be used without any arguments, and its functionality will use
use sane, conservative defaults.  However, it can be customised in various ways
using optional arguments, as follows.

.. automodule:: pypersist
   :members:
