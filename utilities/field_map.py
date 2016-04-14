import os
import arcpy
import logging
import sys

import util
import subtypes_and_domains as sub
import settings


class Field(object):
    """
    Field class.
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
        subtype_field_name, self.subtype_dict = sub.extract_subtype(in_fc, in_source_field)

        # Need to fully qualify the subtype_field in case we're working with a joined FC
        self.subtype_field = _get_full_field_name(subtype_field_name, in_fl)


def build_field_map(in_fc_list, in_field_dict):

    # in_field_dict example:
    # {  'field1': {'out_name':'field77', 'out_length': 23, 'out_type': 23},
    #   'field2': {'out_length':17}
    #   'field3': {'out_name': 'field1214'}  }

    # Thank goodness for this help page
    # http://pro.arcgis.com/en/pro-app/arcpy/classes/fieldmap.htm
    # Field maps suck!)

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


def _join_ini_fields(in_fl, join_list):

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


def _get_full_field_name(in_field_name, in_fl):

    if in_field_name:

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


def _parse_ini_file(in_fl, input_ini):

    field_obj_list = []

    # Remove the join item if it exists-- don't want to create a Field Object for it
    remove_join_dict = {k: v for k, v in input_ini.iteritems() if k != '__joins__'}

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

    # Add an fields required by an equation in the .ini file, but not otherwise included
    # These will need to be copied to the local FC, used for the equation, then deleted
    field_obj_list_final = _add_additional_to_fms(field_obj_list)

    return field_obj_list_final


def _add_additional_to_fms(in_field_list):

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


def set_workspace_from_fl(input_fl):

    # Find SDE path so we can grab relevant tables
    sde_path = os.path.dirname(arcpy.Describe(input_fl).catalogPath)
    desc = arcpy.Describe(sde_path)
    if hasattr(desc, "datasetType") and desc.datasetType == 'FeatureDataset':
        sde_path = os.path.dirname(sde_path)

    # Set workspace so it's easy to list feature classes, tables etc
    arcpy.env.workspace = sde_path


def _field_list_to_fms_dict(in_field_list):

    out_dict = {}

    for field in in_field_list:

        if field.source_field:
            out_dict[field.source_field] = {'out_name': field.out_field_name}

    return out_dict


def _cleanup_output_fc(in_fc, field_list):

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
    temp_str_fields = [f.name for f in arcpy.ListFields(in_fc) if '__string__' in f.name]
    for field_name in temp_str_fields:
        arcpy.AlterField_management(in_fc, field_name, field_name.replace('__string__',''))


def _post_process_fields(in_fc, in_field_list):

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

            temp_field_name = field.out_field_name + '__string__'
            arcpy.AddField_management(in_fc, temp_field_name, 'TEXT', "", "", 254)

            temp_field_obj = Field(temp_field_name)
            in_field_list.append(temp_field_obj)

            cursor_field_list.append(temp_field_name)

        if field.equation_add_field or field.str_val or field.subtype_field:

            with arcpy.da.UpdateCursor(in_fc, cursor_field_list) as cursor:
                for row in cursor:
                    row = _process_row(row, field)
                    cursor.updateRow(row)

    _cleanup_output_fc(in_fc, in_field_list)


def _process_row(in_row, field_obj):

    if field_obj.equation_add_field:
        if in_row[1]:
            in_row[0] = str(int(in_row[1]) * int(field_obj.equation_constant))
        else:
            in_row[0] = None

    elif field_obj.str_val:
        in_row[0] = field_obj.str_val

    elif field_obj.subtype_field:

        # Check if the dictionary is nested (coded value domain for a subtype) or just key/value pairs (working)
        # with a domain only)
        first_value = field_obj.subtype_dict.values()[0]

        if isinstance(first_value, dict):

            if in_row[0]:

                # Grab the value for the field we're trying to update as row[0]
                # The value for the subtype field is row[1]
                # Using the value for the subtype field as a key to the subtype dict,
                # pass in the actual value in row[0] to get the translated val
                in_row[2] = field_obj.subtype_dict[in_row[1]][in_row[0]]

        # If the dictionary isn't nested, we're working with just key/pair values either an actual subtype
        # field or a domain
        else:
            in_row[2] = field_obj.subtype_dict[in_row[1]]

    return in_row


def _validate_fieldmap(in_fc, in_ini_file):

    ini_field_list = [k for k in in_ini_file.keys() if k != '__joins__']

    fc_field_list = [f.name for f in arcpy.ListFields(in_fc) if not f.required and 'Shape' not in f.name]

    if set(ini_field_list) == set(fc_field_list):
        valid = True

    else:
        valid = False

    return valid


def get_ini_dict(path):

    if os.path.splitext(path)[1] == '.ini':
        ini_path = path
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


def ini_fieldmap_to_fc(in_fc, ini_dict, out_workspace):

    # Check to see if the field map is valid before we go through the process
    # This is important because if we reset self.source of a layer, and it has a fieldmap
    # listed in the Google Doc, it will run this routine again
    if _validate_fieldmap(in_fc, ini_dict):
        return in_fc

    else:
        arcpy.MakeFeatureLayer_management(in_fc, 'temp_join_fl')

        # Set workspace so it's easy to find full names of tables listed in join
        # i.e. "concessions" is actually cmr_open_data_en.CMR.concessions
        set_workspace_from_fl('temp_join_fl')

        if '__joins__' in ini_dict:

            # Join the fields to the temporary FL
            _join_ini_fields('temp_join_fl', ini_dict['__joins__'])

        # Convert the ini file into a list of field objects
        # These objects have properties to determine how to treat the fields
        field_list = _parse_ini_file('temp_join_fl', ini_dict)

        # Convert this to the dict format required by the build_field_map functiono
        # This function is used by other modules-- need a consistent format
        fms_dict = _field_list_to_fms_dict(field_list)

        # Convert to a field map
        fms = build_field_map(['temp_join_fl'], fms_dict)

        # Copy the source FC out based on the fieldmap
        out_fc = util.copy_to_scratch_workspace('temp_join_fl', out_workspace, fms)
        arcpy.Delete_management('temp_join_fl')

        # Add additional string or calculated fields to the copied-out fc
        _post_process_fields(out_fc, field_list)

        return out_fc

