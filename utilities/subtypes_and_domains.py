import arcpy


def extract_subtype(in_fc, in_field_name):
    """
    Used to extract all subtypes for a given FC and return those that pertain to a field name of interest
    :param in_fc: fc of interest
    :param in_field_name: field name of interest
    :return: the field that has the subtype, and a dictionary to lookup values for the subtype field and the
    in_field_name to get actual values for our field of interest
    """
    # ID subtype field and build dict based on the value in the att table
    subtypes = arcpy.da.ListSubtypes(in_fc)

    lkp_dict = {}

    # Adapted from esri docs: http://resources.arcgis.com/en/help/main/10.1/index.html#//018w00000021000000
    # Iterate over subtype_val (i.e. integer 1001213) and subtype (a giant dict object)
    # This subtype val is for a particular field. We'll get to that in a second
    for subtype_val, subtype in subtypes.iteritems():

        # Extract name of field that this subtype is for
        subtype_field = subtype['SubtypeField']

        # See if we've already entered this subtype field in the lkp_dict
        try:
            lkp_dict[subtype_field][subtype_val] = {}

        # If we haven't, add it with an empty dictionary as it's value
        except KeyError:
            lkp_dict[subtype_field] = {}
            lkp_dict[subtype_field][subtype_val] = {}

        # If the field we're looking for is actually the subtype field (more of a coded value
        # domain, in this case), just use that for our lookup
        if subtype_field == in_field_name:
            lkp_dict[subtype_field][subtype_val] = subtype['Name']

        # Otherwise iterate over the FieldValues object of the subtype
        else:
            for fieldname, (default_val, domain) in subtype['FieldValues'].iteritems():

                # If the fieldname matches our in_field_name and has a domain
                if fieldname == in_field_name and domain:

                    # Create an empty dict to store the key:value pairs for the domain
                    lkp_dict[subtype_field][subtype_val][fieldname] = {}

                    # Add the keys and values to this dict
                    for domain_key, domain_value in domain.codedValues.iteritems():
                        lkp_dict[subtype_field][subtype_val][fieldname][domain_key] = domain_value

    # Run this through _subtype_to_field_dict to get a field-specific dict
    field_with_subtype, out_dict = _subtype_to_field_dict(lkp_dict)

    return field_with_subtype, out_dict


def _subtype_to_field_dict(in_subtype_dict):
    """
    Take a subtype dict:
    {'type': 1009323: 'gold': 1: yes}
    Return a the field with the subtype and field_dict:
    subtype_field: type
    field_dict: {1009323: {'gold': {0: no, 1: yes}, 1009324: {'gold': {0: no, 1: yes}}
    :param in_subtype_dict: a subtype dict
    :return: a field_dict
    """
    field_dict = {}

    for subtype_field, subtype_value_dict in in_subtype_dict.iteritems():

        # For each value (i.e. 10929340) in the subtype field, grab the associated dict
        for subtype_value, subtype_domain in subtype_value_dict.iteritems():

            if isinstance(subtype_domain, dict):

                # Add the subtype value (i.e. 10929340 as the dict key, with the domain keys/values
                # as the value
                for related_field, field_domain in subtype_domain.iteritems():

                    # logging.debug('Found subtype for field {0}, value: {1}. '
                    #               'Used in field {2}.'.format(subtype_field, subtype_value, related_field))

                    field_dict[subtype_value] = field_domain

            # If this just returns a string, we're working with the subtype field itself
            # Use this string as the value for the subtype key; i.e. subtype 10929340 = string 'Research permits'
            else:
                field_dict[subtype_value] = subtype_domain

    # If we've populated the dict, return it and the subtype field, otherwise return None, None
    if field_dict:
        output = (subtype_field, field_dict)
    else:
        output = (None, None)

    return output
