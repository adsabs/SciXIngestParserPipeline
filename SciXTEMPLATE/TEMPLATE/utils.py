import ast
import json
import os
import sys
import types


def get_schema(app, schema_client, schema_name):
    """
    input:

    app: The relevant calling application (can be any class with a logger attirbute.)
    schema_client: Kafka SchemaRegistryClient
    schema_name: The name of the AVRO schema

    return:

    AVRO schema (str)
    """
    try:
        avro_schema = schema_client.get_latest_version(schema_name)
        app.logger.info("Found schema: {}".format(avro_schema.schema.schema_str))
    except Exception as e:
        avro_schema = None
        app.logger.warning("Could not retrieve avro schema with exception: {}".format(e))

    return avro_schema.schema.schema_str


def load_config(proj_home=None, extra_frames=0, app_name=None):
    """
    Loads configuration from config.py and also from local_config.py

    :parameter proj_home: - str, location of the home - we'll always try
        to load config files from there. If the location is empty,
        we'll inspect the caller and derive the location of its parent
        folder.
    :parameter extra_frames: - int, number of frames to look back; default
        is 2, which is good when the load_config() is called directly,
        but when called from inside classes, we need to add extra more
    :return: dictionary
    """
    conf = {}

    if proj_home is not None:
        proj_home = os.path.abspath(proj_home)
        if not os.path.exists(proj_home):
            raise Exception("{proj_home} doesnt exist".format(proj_home=proj_home))
    else:
        proj_home = os.path.abspath("./")

    if proj_home not in sys.path:
        sys.path.append(proj_home)

    conf["PROJ_HOME"] = proj_home

    conf.update(load_module(os.path.join(proj_home, "config.py")))
    conf.update(load_module(os.path.join(proj_home, "local_config.py")))
    conf_update_from_env(app_name or conf.get("SERVICE", ""), conf)

    return conf


def load_module(filename):
    """
    Loads module, first from config.py then from local_config.py
    :return dictionary
    """

    filename = os.path.join(filename)
    d = types.ModuleType("config")
    d.__file__ = filename
    try:
        with open(filename) as config_file:
            exec(compile(config_file.read(), filename, "exec"), d.__dict__)
    except IOError:
        pass
    res = {}
    from_object(d, res)
    return res


def conf_update_from_env(app_name, conf):
    app_name = app_name.replace(".", "_").upper()
    for key in list(conf.keys()):
        specific_app_key = "_".join((app_name, key))
        if specific_app_key in os.environ:
            # Highest priority: variables with app_name as prefix
            _replace_value(conf, key, os.environ[specific_app_key])
        elif key in os.environ:
            _replace_value(conf, key, os.environ[key])


def from_object(from_obj, to_obj):
    """Updates the values from the given object.  An object can be of one
    of the following two types:
    Objects are usually either modules or classes.
    Just the uppercase variables in that object are stored in the config.
    :param obj: an import name or object
    """
    for key in dir(from_obj):
        if key.isupper():
            to_obj[key] = getattr(from_obj, key)


def _replace_value(conf, key, new_value):
    try:
        w = json.loads(new_value)
        conf[key] = w
    except Exception:
        try:
            # Interpret numbers, booleans, etc...
            conf[key] = ast.literal_eval(new_value)
        except Exception:
            # String
            conf[key] = new_value
