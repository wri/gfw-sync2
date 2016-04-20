import os
import arcpy
import logging

import util
import settings
import subtypes_and_domains as sub


class Field(object):
    """
    Field class used to keep track of various operations needed to execute the crazy things we allow in the
    fieldmaps (joins, string calculations, and multiplication, to name a few)
    """
    def __init__(self, out_field_name):

        self.out_field_name = out_field_name
        self.str_val = None
        self.equation_add_field = None
        self.equation_constant = None
        self.source_field = None
        self.to_delete = None
        self.subtype_field = None
        self.subtype_dict = None

    def populate_subtypes_and_domains(self, in_fc, in_source_field, in_fl):
        """
        Find the subtype field name and the associated subtype_dict for the in_source_field of interest
        This field may be named something different due to joins, so get it's full field name from the in_fl
        :param in_fc: input feature class
        :param in_source_field: the source field we need to check
        :param in_fl: the feature layer of interest-- may have joins etc
        :return:
        """
        subtype_field_name, self.subtype_dict = sub.extract_subtype(in_fc, in_source_field)

        # Need to fully qualify the subtype_field in case we're working with a joined FC
        self.subtype_field = _get_full_field_name(subtype_field_name, in_fl)


def get_ini_dict(path):
    """
    Simple function to take a path to an ini file and return the ini dict
    If the path does not end in .ini, it's actually pointing to a specific dict within the file
    If this is the case, parse the .ini file path and then return the dict of interest
    :param path:
    :return:
    """

    if os.path.splitext(path)[1] == '.ini':
        ini_dict = settings.get_ini_file(path)

    else:
        # Find ini file in path
        # http://stackoverflow.com/questions/3167154/how-to-split-a-dos-path-into-its-components-in-python#answer-16595356
        path = os.path.normpath(path)
        split_list = path.split(os.sep)

        ini_path_list = []
        key_list = []

        ini_ext_found = False
        for s in split_list:

            if ini_ext_found:
                key_list.append(s)

            else:
                ini_path_list.append(s)

            if os.path.splitext(s)[1] == '.ini':
                ini_ext_found = True

        # Required due to the way windows drives work
        ini_file_path = os.path.join(ini_path_list[0] + os.sep, *ini_path_list[1:])

        # Get the dict for the entire file before subsetting it
        entire_dict = settings.get_ini_file(ini_file_path)

        # Subset it based on the keys supplied in the path above
        # http://stackoverflow.com/questions/14692690/access-python-nested-dictionary-items-via-a-list-of-keys
        ini_dict = reduce(lambda d, k: d[k], key_list, entire_dict)

    return ini_dict


def build_field_map(in_fc_list, in_field_dict):
    """
    Given an in_field_dict of format:
    {  'field1': {'out_name':'field77', 'out_length': 23, 'out_type': 23},
       'field2': {'out_length':17}
       'field3': {'out_name': 'field1214'}  }

    Build an arcpy field mappings object to map fields for the in_fc_list to this output
    Help: http://pro.arcgis.com/en/pro-app/arcpy/classes/fieldmap.htm

    :param in_fc_list: list of FCs for which to generate the fieldmap. A list object in case we're merging FCs
    :param in_field_dict: a dict that matches the specs above
    :return: an arcpy.FieldMappings() object
    """

    # Create our field mappings object
    fms = arcpy.FieldMappings()

    # Iterate over all the fields we're interested in bringing to the next fc
    for field_name in in_field_dict.keys():

        # Create a fieldmap object
        # This must be done outside the for fc in in_fc_list for loop
        # We need to create one fieldmap (fm) per field, not one per field per fc
        fm = arcpy.FieldMap()

        # Iterate over all of our FCs of interest to see if they have that field
        # If they do, we'll add it to the field map for that field
        for fc in in_fc_list:

            # Pull out the field object if it exists. We'll modify this to set a new name and length
            # and then push it back to the field mapping
            current_field_list = [f for f in arcpy.ListFields(fc) if f.name == field_name]

            # If this field exists in the fc of interest
            if len(current_field_list) == 1:
                current_field = current_field_list[0]

                # Add the field from this FC to the list of fields to be merged in the output dataset
                fm.addInputField(fc, field_name)

                # If we've specified a new name, set it
                if 'out_name' in in_field_dict[field_name]:
                    current_field.name = in_field_dict[field_name]['out_name']

                # If we've specified an out length, set it
                # Otherwise will go with default
                if 'out_length' in in_field_dict[field_name]:
                    current_field.length = in_field_dict[field_name]['out_length']

                # If we've specified an out length, set it
                # Otherwise will go with default
                if 'out_type' in in_field_dict[field_name]:
                    current_field.type = in_field_dict[field_name]['out_type']

                # Set the output field to the updated field definition
                fm.outputField = current_field

            else:
                logging.debug('Field {0} not found in input fc {1} during field mapping'.format(field_name, fc))

                fm = None

        # CRITICAL NOTE
        # Only add the fieldmap object to the field mappings object once! This is key-- otherwise
        # will get duplicate field names. Make sure you know where it is in the for loop-- one field map
        # per field of interest (duh)
        if fm:
            fms.addFieldMap(fm)

    return fms


def _fieldmap_already_complete(in_fc, ini_dict):
    """
    Check if the in_fc already matches the ini_dict specified
    :param in_fc: the fc to execute a field map on
    :param ini_dict: the dict from a .ini file that specifies a field map
    :return:
    """

    ini_field_list = [k for k in ini_dict.keys() if k != '__joins__']
    fc_field_list = [f.name for f in arcpy.ListFields(in_fc) if not f.required and 'Shape' not in f.name]

    if set(ini_field_list) == set(fc_field_list):
        fm_already_complete = True

    else:
        fm_already_complete = False

    return fm_already_complete


def set_workspace_from_fl(input_fl):
    """
    Sets the workspace from the feature layer. Important because we need to find the full paths of other
    tables and feature classes in the workspace if executing joins
    :param input_fl:
    :return:
    """

    # Find SDE path so we can grab relevant tables
    sde_path = os.path.dirname(arcpy.Describe(input_fl).catalogPath)
    desc = arcpy.Describe(sde_path)
    if hasattr(desc, "datasetType") and desc.datasetType == 'FeatureDataset':
        sde_path = os.path.dirname(sde_path)

    # Set workspace so it's easy to list feature classes, tables etc
    arcpy.env.workspace = sde_path


def _join_ini_fields(in_fl, join_list):
    """
    Execute any joins specified by the ini dict
    :param in_fl: the feature layer we're operating on
    :param join_list: a list of joins, from the __joins__ keyword in the .ini dict. Example:
            __joins__ = nom_conces ON concessions.nom_conces,concessions.attributai ON societes.societe
    :return:
    """

    for join in join_list:
        src_info, to_info = join.split(' ON ')
        to_table, to_field = to_info.split('.')

        # Handle any fields that were originally from other tables and are now joined to the feature layer
        # i.e. concessions.attributai becomes cmr_open_data_en.CMR.concessions.attributai
        if '.' in src_info:
            src_field = _get_full_field_name(src_info, in_fl)
        else:
            src_field = src_info

        logging.debug("Joining field {0} in source dataset to {1} "
                      "in {2}".format(src_field, to_table, to_field))
        arcpy.AddJoin_management(in_fl, src_field, to_table, to_field, "KEEP_ALL")


def _ini_dict_to_field_objects(in_fl, input_ini_dict):
    """
    Build Field objects for each output field listed in the .ini dict
    This will parse the input .ini dict so we know what is required for each field-- if it is a basic one to one
    field mapping, or a string calculation, or a multiplication calculation-- and add this info to each Field object
    :param in_fl: the feature layer we're working on
    :param input_ini_dict: the input_ini_dict
    :return:
    """

    field_obj_list = []

    # Remove the join item if it exists-- don't want to create a Field Object for it
    remove_join_dict = {k: v for k, v in input_ini_dict.iteritems() if k != '__joins__'}

    for out_field_name, src_info in remove_join_dict.iteritems():

        # Create a field object for each output field in the final dataset
        field = Field(out_field_name)

        # Fields that will be added later and calculated to a set string will have " or '
        if '"' in src_info or '"' in src_info:
            field.str_val = src_info.replace('"', '').replace("'", '')

        # Find equation fields
        elif '*' in src_info:

            # Example src_info = sup_adm_km2 * 100
            # Iterate over the sup_adm_km2 and 100 (for this example, anyway)
            for x in src_info.split('*'):
                try:
                    # Find the value part of the expression (i.e. the 100 in field_name * 100)
                    field.equation_constant = float(x.strip())

                # Find the half of the equation that has the input field name in it
                # We'll use this as a placeholder; ultimately this will be multiplied by 100 in the local copy
                # of the dataset; just don't want to do this to the source
                except ValueError:
                    field.equation_add_field = _get_full_field_name(x.strip(), in_fl)

        # Otherwise if it's a straightforward field to field mapping, find the full table name
        # for the source field.
        else:
            field.source_field = _get_full_field_name(src_info, in_fl)

        # Append the Field to the list
        field_obj_list.append(field)

    for field_obj in field_obj_list:

        if field_obj.source_field:

            if '.' in field_obj.source_field:

                # Get the table wihout the fieldname
                source_table = '.'.join(field_obj.source_field.split('.')[0:-1])
                source_field = field_obj.source_field.split('.')[-1]

            else:
                source_table = arcpy.Describe(in_fl).catalogPath
                source_field = field_obj.source_field

            # Need to use this to find the subtypes; if a feature layer has joins, it doesn't
            # have any subtypes associated, even if the source fcs/tables do
            field_obj.populate_subtypes_and_domains(source_table, source_field, in_fl)

    # Add any fields required by an equation in the .ini file, but not otherwise included
    # These will need to be copied to the local FC, used for the equation, then deleted
    field_obj_list = _add_additional_to_fms(field_obj_list)

    return field_obj_list


def _get_full_field_name(in_field_name, in_fl):
    """
    Given that we may be working with fields from various tables thanks to the __join__ options, this will
    get the fully qualified fieldname (often including the table name) for the field of interest
    Example: concessions.attributai becomes cmr_open_data_en.CMR.concessions.attributai
    :param in_field_name: the field name from the .ini file
    :param in_fl: the feature layer we're working on
    :return: the fully qualified field name
    """

    if in_field_name:

        # If field name from the .ini dict involes a '.', track down the actual name of the source table
        if '.' in in_field_name:

            # If this field is from another (joined) table, get it's full field name
            in_table_name, field_name = in_field_name.split('.')
            wild_card_str = "*{0}*".format(in_table_name)

            # Assume that this is a table, but could also be joining to a feature class
            try:
                full_table_name = arcpy.ListTables(wild_card_str)[0]
            except IndexError:
                full_table_name = arcpy.ListFeatureClasses(wild_card_str)[0]

            full_field_name = '{0}.{1}'.format(full_table_name, field_name)

        # Otherwise it's from the source FC
        else:

            # If there are joins, need to qualify it with the full name of the FC
            if _has_joins(in_fl):
                field_name = in_field_name
                full_table_name = os.path.basename(arcpy.Describe(in_fl).catalogPath)
                full_field_name = '{0}.{1}'.format(full_table_name, field_name)

            # If no joins, can just use the regular field name
            else:
                full_field_name = in_field_name

    else:
        full_field_name = None

    return full_field_name


def _has_joins(in_fl):
    """
    Check if joins are present based on the fields listed in in the table
    :param in_fl: feature layer of interest
    :return: boolean if joins are present in this feature class
    """

    # If joins are present, the name of the actual dataset will be visible in field names
    # i.e. if a regular field in not-joined dataset cmr.forest.prod_forest is area_ha
    # that field will be called cmr.forest.prod_forest.area_ha in a joined dataset
    src_table = os.path.basename(arcpy.Describe(in_fl).catalogPath)
    joined_fields = [f.name for f in arcpy.ListFields(in_fl) if src_table in f.name]

    if joined_fields:
        has_joins = True
    else:
        has_joins = False

    return has_joins


def _add_additional_to_fms(in_field_list):
    """
    Add any fields required by an equation in the .ini file, but not otherwise included
    These will need to be copied to the local FC, used for the equation, then deleted
    :param in_field_list: list of field objects
    :return: a list of field objects that may contain new fields
    """

    # Grab all the fields we have currently
    current_field_list = [f.out_field_name for f in in_field_list]

    # Grab all the fields with an equation_add_field specified
    add_field_list = [f for f in in_field_list if f.equation_add_field or f.subtype_field]

    for add_field in add_field_list:

        # Filter the attributes to get the new field we want to add
        # Each field object can be either an equation field or a subtype field, not both
        # As a result, this will return one fieldname
        new_field_name = filter(None, (add_field.equation_add_field, add_field.subtype_field))[0]

        # If the new field isn't in our field list, add it
        if new_field_name not in current_field_list:

            # Remove the table name from the field if it exists-- field in the out table
            # should just be the name (i.e. area_ha, not cmr.open_data.forests.area_ha)
            final_field_name = new_field_name.split('.')[-1]

            # Create a new Field object for this field and add it's source field
            new_field_obj = Field(final_field_name)

            # Copy the field directly from the current source to the output table
            new_field_obj.source_field = new_field_name

            # Carry the subtype_dict over-- important so that when we remove it we can delete
            # the subtype first. ugh.
            new_field_obj.subtype_dict = add_field.subtype_dict

            # If this field wasn't already in the output list, not needed in final table
            # We'll use it to calculate a field in the output, then delete it
            new_field_obj.to_delete = True

            in_field_list.append(new_field_obj)

            # Add the field_name to the current list so we don't add it twice by mistake
            current_field_list += [new_field_name]

    return in_field_list


def _field_list_to_fms_dict(in_field_list):
    """
    Convert a list of field objects to the fms_dict format required by build_field_map()
    Example output:
    {  'field1': {'out_name':'field77', 'out_length': 23, 'out_type': 23},
       'field2': {'out_length':17}
       'field3': {'out_name': 'field1214'}  }
    :param in_field_list: list of field objects
    :return: dict in proper format
    """

    out_dict = {}

    for field in in_field_list:

        if field.source_field:
            out_dict[field.source_field] = {'out_name': field.out_field_name}

    return out_dict


def _post_process_fields(in_fc, in_field_list):
    """
    Updates fields just defined as a string value in the .ini file (i.e. source = '"Minfof"', as well as
     calculated fields (i.e. area_ha = area_m * 10000) and fields with subtype dicts associated
    :param in_fc: the output FC that has already had field mapping applied
    :param in_field_list: list of Field objects
    :return:
    """

    logging.debug("Post processing the FC after field mapping")

    for field in in_field_list:

        # Build a field list for the cursor-- we'll need to at least use the
        # field of interest so we can update it
        cursor_field_list = [field.out_field_name]

        # If the field is already in our output FC, don't need to add it again
        if field.out_field_name not in [f.name for f in arcpy.ListFields(in_fc)]:
            # Add the out_field_name to the feature class-- was not present inintially
            arcpy.AddField_management(in_fc, field.out_field_name, "TEXT", "", "", 254)

        if field.equation_add_field:

            # Need to make sure we just get the field name, not the table_name.field_name
            # We're working with the output FC now, so field names no longer include the
            # table they came from
            field.equation_add_field = field.equation_add_field.split('.')[-1]

            # Add the other field required by the equation
            cursor_field_list.append(field.equation_add_field)

        elif field.subtype_field:

            # Add the field with the subtype
            # Strip out any additional field that was used in the join process
            current_subtype_field = field.subtype_field.split('.')[-1]
            cursor_field_list.append(current_subtype_field)

            # Delete the field we've copied over
            # Will be replaced by the field below that we know is a string
            field.to_delete = True

            # Add a string field with the name {fieldname}__string__
            # This is ncessary given that the coded domain values are strings,
            # but the fields themselves are integers, therefore need an additional
            # text field to update with the proper value
            temp_field_name = field.out_field_name + '__string__'
            arcpy.AddField_management(in_fc, temp_field_name, 'TEXT', "", "", 254)

            # Add it to the list of Field objects, and to the cursor_field_list
            temp_field_obj = Field(temp_field_name)
            in_field_list.append(temp_field_obj)
            cursor_field_list.append(temp_field_name)

        if field.equation_add_field or field.str_val or field.subtype_field:

            # Iterate row by row over the FC
            with arcpy.da.UpdateCursor(in_fc, cursor_field_list) as cursor:
                for row in cursor:
                    row = _process_row(row, field)
                    cursor.updateRow(row)

    _cleanup_output_fc(in_fc, in_field_list)


def _process_row(in_row, field_obj):
    """
    Evaluate field_obj and process the row accordingly-- may set to a string value, may multiple a field, may
    use a lookup from a subtype/domain to set the value
    :param in_row: a row object from an UpdateCursor
    :param field_obj: a Field object
    :return: an updated row
    """

    if field_obj.equation_add_field:
        if in_row[1]:

            # If row[1] is not null, multiply it by the field_obj.equation_constant
            in_row[0] = str(int(in_row[1]) * int(field_obj.equation_constant))
        else:
            in_row[0] = None

    elif field_obj.str_val:

        # If we're just setting it to a string value, do so
        in_row[0] = field_obj.str_val

    elif field_obj.subtype_field:

        # Check if the dictionary is nested (coded value domain for a subtype) or just key/value pairs (working)
        # with a domain only)
        first_value = field_obj.subtype_dict.values()[0]

        if isinstance(first_value, dict) and in_row[0]:

            # Grab the value for the field we're trying to update as row[0] if it's not null
            # The value for the subtype field is row[1]
            # Using the value for the subtype field as a key to the subtype dict,
            # pass in the actual value in row[0] to get the translated val
            in_row[2] = field_obj.subtype_dict[in_row[1]][in_row[0]]

        elif isinstance(first_value, dict) and not in_row[0]:
            # NULL value for in_row[0]-- nothing we can use to input into our domain
            pass

        # If the dictionary isn't nested, we're working with just key/pair values either an actual subtype
        # field or a domain
        else:
            in_row[2] = field_obj.subtype_dict[in_row[1]]

    return in_row


def _cleanup_output_fc(in_fc, field_list):
    """
    Delete fields with the to_delete flag, generally those associated with multiplication, but that aren't
    in the final output fc. Same goes for subtype fields
    :param in_fc:
    :param field_list:
    :return:
    """

    for field in field_list:
        if field.to_delete:

            # Have to remove the subtypes from the GDB before deleting
            if field.subtype_dict:

                # Subtypes exist for this dataset, but (for some reason) will throw an error if we try to delete them
                if arcpy.da.ListSubtypes(in_fc).keys() == [0]:
                    pass

                # Subtypes exist and need to be deleted
                else:
                    arcpy.RemoveSubtype_management(in_fc, field.subtype_dict.keys())

            arcpy.DeleteField_management(in_fc, field.out_field_name)

    # After we remove all the fields to delete, check if any __string__ fields are present
    # These are added above to handle domains/subtypes-- their source fields are integers, but output
    # values are strings. We update the __string__ fields to the correct value, then delete the original
    # coded value field and then rename the __string__ field to the proper name. Ugh.
    temp_str_fields = [f.name for f in arcpy.ListFields(in_fc) if '__string__' in f.name]
    for field_name in temp_str_fields:
        arcpy.AlterField_management(in_fc, field_name, field_name.replace('__string__', ''))


def ini_fieldmap_to_fc(in_fc, dataset_name, ini_dict, out_workspace):
    """
    Take an ini fieldmap and conver to FC. Can include __joins__, calculated fields (with * only), and fields to
    set to string values
    :param in_fc: the source FC to field map
    :param dataset_name: the name to use for the feature layer
    :param ini_dict: the input dictionary from the .ini file
    :param out_workspace: the workspace to write the output FC to
    :return: the output FC
    """

    if not os.path.exists(out_workspace):
        os.mkdir(out_workspace)

    # Check to see if the field mapping has alrady been execute for this FC
    # This is important because if we reset self.source of a layer, and it has a fieldmap
    # listed in the Google Doc, it will run this routine again
    if _fieldmap_already_complete(in_fc, ini_dict):
        out_fc = in_fc

    else:
        dataset_fl = dataset_name + '_fl'
        arcpy.MakeFeatureLayer_management(in_fc, dataset_fl)

        # Set workspace so it's easy to find full names of tables listed in join
        # i.e. "concessions" is actually cmr_open_data_en.CMR.concessions
        set_workspace_from_fl(dataset_fl)

        if '__joins__' in ini_dict:

            # Join the fields to the temporary FL
            _join_ini_fields(dataset_fl, ini_dict['__joins__'])

        # Convert the ini dict into a list of field objects
        # These objects have properties to determine how to treat the fields
        field_list = _ini_dict_to_field_objects(dataset_fl, ini_dict)

        # Convert this to the dict format required by the build_field_map functiono
        # This function is used by other modules-- need a consistent format
        fms_dict = _field_list_to_fms_dict(field_list)

        # Convert to a field map
        fms = build_field_map([dataset_fl], fms_dict)

        # Copy the source FC out based on the fieldmap
        out_fc = util.copy_to_scratch_workspace(dataset_fl, out_workspace, fms)
        arcpy.Delete_management(dataset_fl)

        # Add additional string or calculated fields to the copied-out fc
        _post_process_fields(out_fc, field_list)

    return out_fc

