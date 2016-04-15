import arcpy


def extract_subtype(in_fc, in_field_name):
    # ID subtype field and build dict based on the value in the att table
    subtypes = arcpy.da.ListSubtypes(in_fc)

    lkp_dict = {}

    for subtype_val, subtype in subtypes.iteritems():

        subtype_field = subtype['SubtypeField']

        try:
            lkp_dict[subtype_field][subtype_val] = {}
        except KeyError:
            lkp_dict[subtype_field] = {}
            lkp_dict[subtype_field][subtype_val] = {}

        # If the field we're looking for is actually the subtype field (more of a coded value
        # domain, in this case), just use that for our lookup
        if subtype_field == in_field_name:
            lkp_dict[subtype_field][subtype_val] = subtype['Name']

        else:
            for fieldname, (default_val, domain) in subtype['FieldValues'].iteritems():

                if fieldname == in_field_name and domain:

                    lkp_dict[subtype_field][subtype_val][fieldname] = {}

                    for domain_key, domain_value in domain.codedValues.iteritems():

                        lkp_dict[subtype_field][subtype_val][fieldname][domain_key] = domain_value

    field_with_subtype, out_dict = _subtype_to_field_dict(lkp_dict)

    return field_with_subtype, out_dict


def _subtype_to_field_dict(in_subtype_dict):
    # field.subtypes.lkp_dict = {'fieldval1': lookupdict, 'fieldval2': lookupdict, 'fieldval3': lookupdict"
    field_dict = {}

    for subtype_field, subtype_value_dict in in_subtype_dict.iteritems():

        for subtype_value, subtype_domain in subtype_value_dict.iteritems():

            if isinstance(subtype_domain, dict):

                for related_field, field_domain in subtype_domain.iteritems():

                    # logging.debug('Found subtype for field {0}, value: {1}. '
                    #               'Used in field {2}.'.format(subtype_field, subtype_value, related_field))

                    field_dict[subtype_value] = field_domain

            # If this just returns a string, we're working with the subtype field itself
            # Use this string as the value for the subtype key; i.e. subtype 23480000 = string 'Research permits'
            else:
                field_dict[subtype_value] = subtype_domain

    if field_dict:
        output = (subtype_field, field_dict)
    else:
        output = (None, None)

    return output
