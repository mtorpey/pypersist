namespace_t = ["gapmemo", "pymemo"]

memo_schema = {
    # Schema definition, based on Cerberus grammar. Check the Cerberus project
    # (https://github.com/pyeve/cerberus) for details.
    'funcname': {
        'type': 'string',
        'minlength': 1,
        'required': True
    },
    'hash': {
        'type': 'string',
        'required': True
    },
    'namespace': {
        'type': "string",
        'required': True
    },
    'result': {
        'type': 'string',
        'required': True
    },
    'metadata': {
        'type': 'dict',
        'required': False
    },
    'comments': {
        'type': 'string',
        'required': False
    }
}

memos = {
    'url': 'memos/<regex("[\w]+"):funcname>',

    # by default the standard item entry point is defined as
    # '/people/<ObjectId>'. We leave it untouched, and we also enable an
    # additional read-only entry point. This way consumers can also perform
    # GET requests at '/people/<lastname>'.
    'additional_lookup': {
        'url': 'regex("[-\w]+")',
        'field': 'hash',
    },

    # We choose to override global cache-control directives for this resource.
    #'cache_control': 'max-age=10,must-revalidate',
    #'cache_expires': 10,

    # most global settings can be overridden at resource level
    #'resource_methods': ['GET', 'POST'],

    'schema': memo_schema
}

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

MONGO_DBNAME = 'memoisation'    # Database name
DOMAIN = {'memos': memos}    # Collection name

RESOURCE_METHODS = ['GET', 'POST', 'DELETE']
ITEM_METHODS = ['GET', 'PATCH', 'PUT', 'DELETE']
