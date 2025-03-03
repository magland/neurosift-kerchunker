from typing import Any
import re
import pynwb
from NWBTyped import NWBTyped


def get_nwb_usage_string(nwb: NWBTyped, *, header: str = "") -> str:
    nwb = NWBTyped(nwb)  # in case it came in as a pynwb nwb file
    object_strings = []
    s = ""
    if header:
        s += f"{header}\n"
        s += "\n"
    s += f"nwb.session_description # (str) {nwb.session_description}\n"
    s += f"nwb.identifier # (str) {nwb.identifier}\n"
    s += f"nwb.session_start_time # (datetime) {nwb.session_start_time}\n"
    s += f"nwb.file_create_date # (datetime) {nwb.file_create_date}\n"
    s += f"nwb.timestamps_reference_time # (datetime) {nwb.timestamps_reference_time}\n"
    s += f"nwb.experimenter # (List[str]) {nwb.experimenter}\n"
    s += f"nwb.experiment_description # (str) {nwb.experiment_description}\n"
    s += f"nwb.session_id # (str) {nwb.session_id}\n"
    s += f"nwb.institution # (str) {nwb.institution}\n"
    s += f"nwb.keywords # (List[str]) {nwb.keywords}\n"
    if nwb.notes:
        s += f"nwb.notes # (str) {nwb.notes}\n"
    if nwb.pharmacology:
        s += f"nwb.pharmacology # (str) {nwb.pharmacology}\n"
    if nwb.protocol:
        s += f"nwb.protocol # (str) {nwb.protocol}\n"
    if nwb.related_publications:
        s += f"nwb.related_publications # (List[str]) {nwb.related_publications}\n"
    if nwb.slices:
        s += f"nwb.slices # (str) {nwb.slices}\n"
    if nwb.source_script:
        s += f"nwb.source_script # (str) {nwb.source_script}\n"
    if nwb.source_script_file_name:
        s += f"nwb.source_script_file_name # (str) {nwb.source_script_file_name}\n"
    if nwb.data_collection:
        s += f"nwb.data_collection # (str) {nwb.data_collection}\n"
    if nwb.surgery:
        s += f"nwb.surgery # (str) {nwb.surgery}\n"
    if nwb.virus:
        s += f"nwb.virus # (str) {nwb.virus}\n"
    if nwb.stimulus_notes:
        s += f"nwb.stimulus_notes # (str) {nwb.stimulus_notes}\n"
    if nwb.lab:
        s += f"nwb.lab # (str) {nwb.lab}\n"
    if nwb.acquisition:
        for k in nwb.acquisition.keys():
            objstr = get_object_string(k, f'nwb.acquisition["{k}"]', nwb.acquisition[k])
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.acquisition[k], "description", "")
            s += f'nwb.acquisition["{k}"] # ({type(nwb.acquisition[k]).__name__}) {description}\n'
    if nwb.analysis:
        for k in nwb.analysis.keys():
            objstr = get_object_string(k, f'nwb.analysis["{k}"]', nwb.analysis[k])
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.analysis[k], "description", "")
            s += f'nwb.analysis["{k}"] # ({type(nwb.analysis[k]).__name__}) {description}\n'
    if nwb.stimulus:
        for k in nwb.stimulus.keys():
            objstr = get_object_string(k, f'nwb.stimulus["{k}"]', nwb.stimulus[k])
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.stimulus[k], "description", "")
            s += f'nwb.stimulus["{k}"] # ({type(nwb.stimulus[k]).__name__}) {description}\n'
    if nwb.stimulus_template:
        for k in nwb.stimulus_template.keys():
            objstr = get_object_string(
                k, f'nwb.stimulus_template["{k}"]', nwb.stimulus_template[k]
            )
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.stimulus_template[k], "description", "")
            s += f'nwb.stimulus_template["{k}"] # ({type(nwb.stimulus_template[k]).__name__}) {description}\n'
    if nwb.epochs:
        s += f"nwb.epochs # ({type(nwb.epochs).__name__})\n"
    if nwb.trials:
        s += f"nwb.trials # ({type(nwb.trials).__name__})\n"
    if nwb.invalid_times:
        s += f"nwb.invalid_times # ({type(nwb.invalid_times).__name__})\n"
    if nwb.intervals:
        for k in nwb.intervals.keys():
            objstr = get_object_string(k, f'nwb.intervals["{k}"]', nwb.intervals[k])
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.intervals[k], "description", "")
            s += f'nwb.intervals["{k}"] # ({type(nwb.intervals[k]).__name__}) {description}\n'
    if nwb.units:
        s += f"nwb.units # ({type(nwb.units).__name__})\n"
    if nwb.processing:
        for k in nwb.processing.keys():
            objstr = get_object_string(k, f'nwb.processing["{k}"]', nwb.processing[k])
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.processing[k], "description", "")
            s += f'nwb.processing["{k}"] # ({type(nwb.processing[k]).__name__}) {description}\n'
    if nwb.lab_meta_data:
        for k in nwb.lab_meta_data.keys():
            objstr = get_object_string(
                k, f'nwb.lab_meta_data["{k}"]', nwb.lab_meta_data[k]
            )
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.lab_meta_data[k], "description", "")
            s += f'nwb.lab_meta_data["{k}"] # ({type(nwb.lab_meta_data[k]).__name__}) {description}\n'
    if nwb.electrodes:
        s += f"nwb.electrodes # ({type(nwb.electrodes).__name__})\n"
    if nwb.electrode_groups:
        s += f"nwb.electrode_groups # (List[str]) {nwb.electrode_groups}\n"
    if nwb.sweep_table:
        s += f"nwb.sweep_table # ({type(nwb.sweep_table).__name__})\n"
    if nwb.imaging_planes:
        for k in nwb.imaging_planes.keys():
            objstr = get_object_string(
                k, f'nwb.imaging_planes["{k}"]', nwb.imaging_planes[k]
            )
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.imaging_planes[k], "description", "")
            s += f'nwb.imaging_planes["{k}"] # ({type(nwb.imaging_planes[k]).__name__}) {description}\n'
    if nwb.ogen_sites:
        for k in nwb.ogen_sites.keys():
            objstr = get_object_string(k, f'nwb.ogen_sites["{k}"]', nwb.ogen_sites[k])
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.ogen_sites[k], "description", "")
            s += f'nwb.ogen_sites["{k}"] # ({type(nwb.ogen_sites[k]).__name__}) {description}\n'
    if nwb.devices:
        for k in nwb.devices.keys():
            objstr = get_object_string(k, f'nwb.devices["{k}"]', nwb.devices[k])
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.devices[k], "description", "")
            s += f'nwb.devices["{k}"] # ({type(nwb.devices[k]).__name__}) {description}\n'
    if nwb.subject:
        s += f"nwb.subject # ({type(nwb.subject).__name__})\n"
    if nwb.scratch:
        for k in nwb.scratch.keys():
            objstr = get_object_string(k, f'nwb.scratch["{k}"]', nwb.scratch[k])
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.scratch[k], "description", "")
            s += f'nwb.scratch["{k}"] # ({type(nwb.scratch[k]).__name__}) {description}\n'
    if nwb.icephys_electrodes:
        for k in nwb.icephys_electrodes.keys():
            objstr = get_object_string(
                k, f'nwb.icephys_electrodes["{k}"]', nwb.icephys_electrodes[k]
            )
            if objstr:
                object_strings.append(objstr)
            description = getattr(nwb.icephys_electrodes[k], "description", "")
            s += f'nwb.icephys_electrodes["{k}"] # ({type(nwb.icephys_electrodes[k]).__name__}) {description}\n'
    if nwb.intracellular_recordings:
        s += f"nwb.intracellular_recordings # ({type(nwb.intracellular_recordings).__name__})\n"
    if nwb.icephys_simultaneous_recordings:
        s += f"nwb.icephys_simultaneous_recordings # ({type(nwb.icephys_simultaneous_recordings).__name__})\n"
    if nwb.icephys_sequential_recordings:
        s += f"nwb.icephys_sequential_recordings # ({type(nwb.icephys_sequential_recordings).__name__})\n"
    if nwb.icephys_repetitions:
        s += f"nwb.icephys_repetitions # ({type(nwb.icephys_repetitions).__name__})\n"
    if nwb.icephys_experimental_conditions:
        s += f"nwb.icephys_experimental_conditions # ({type(nwb.icephys_experimental_conditions).__name__})\n"

    # subject
    s += "\n"
    if nwb.subject.age:  # type: ignore
        s += f"nwb.subject.age # (str) {str(nwb.subject.age)}\n"  # type: ignore
    if nwb.subject.age__reference:  # type: ignore
        s += f"nwb.subject.age__reference # (str) {nwb.subject.age__reference}\n"  # type: ignore
    if nwb.subject.description:  # type: ignore
        s += f"nwb.subject.description # (str) {nwb.subject.description}\n"  # type: ignore
    if nwb.subject.genotype:  # type: ignore
        s += f"nwb.subject.genotype # (str) {nwb.subject.genotype}\n"  # type: ignore
    if nwb.subject.sex:  # type: ignore
        s += f"nwb.subject.sex # (str) {nwb.subject.sex}\n"  # type: ignore
    if nwb.subject.species:  # type: ignore
        s += f"nwb.subject.species # (str) {nwb.subject.species}\n"  # type: ignore
    if nwb.subject.subject_id:  # type: ignore
        s += f"nwb.subject.subject_id # (str) {nwb.subject.subject_id}\n"  # type: ignore
    if nwb.subject.weight:  # type: ignore
        s += f"nwb.subject.weight # (str) {nwb.subject.weight}\n"  # type: ignore
    if nwb.subject.date_of_birth:  # type: ignore
        s += f"nwb.subject.date_of_birth # (datetime) {nwb.subject.date_of_birth}\n"  # type: ignore
    if nwb.subject.strain:  # type: ignore
        s += f"nwb.subject.strain # (str) {nwb.subject.strain}\n"  # type: ignore

    for objstr in object_strings:
        s += f"\n{objstr}"

    # s += '\n'
    # s += '# NOTES:\n'

    return s


def get_object_string(varname: str, varexpr: str, obj: Any) -> str:
    varname = make_valid_varname(varname)
    description = getattr(obj, "description", "")
    s = ""
    if type(obj).__name__ == "TimeSeries":
        assert isinstance(obj, pynwb.base.TimeSeries)
        if description:
            s += f"# {description}\n"
        s += f"{varname} = {varexpr}\n"
        if hasattr(obj, "timestamps"):
            timestamps = obj.timestamps
            shape = (
                getattr(timestamps, "shape", None) if timestamps is not None else None
            )
            s += f"{varname}.timestamps # shape: {shape}\n"
        else:
            s += f"{varname}.starting_time # {obj.starting_time}\n"
            s += f"{varname}.rate # {obj.rate}\n"  # type: ignore
        data = obj.data
        shape = getattr(data, "shape", None) if data is not None else None
        dtype = getattr(data, "dtype", None) if data is not None else None
        s += f"{varname}.data # shape: {shape}; dtype: {dtype}\n"
    elif type(obj).__name__ == "TimeIntervals":
        assert isinstance(obj, pynwb.epoch.TimeIntervals)
        if description:
            s += f"# {description}\n"
        s += f"{varname} = {varexpr}\n"
        start_time = obj["start_time"].data
        stop_time = obj["stop_time"].data
        s += f'{varname}["start_time"].data # (h5py.Dataset) shape: {start_time.shape}; dtype: {start_time.dtype};\n'
        s += f'{varname}["stop_time"].data # (h5py.Dataset) shape: {stop_time.shape}; dtype: {stop_time.dtype};\n'
        # todo: handle tags
    return s


def make_valid_varname(s: str) -> str:
    return re.sub(r"\W|^(?=\d)", "_", s)
