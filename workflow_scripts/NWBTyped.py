import pynwb
from typing import Optional, List, Any, Dict
from datetime import datetime
import numpy as np
import h5py
import hdmf
from hdmf.common.table import DynamicTable
from pynwb.epoch import TimeIntervals
from pynwb.misc import Units
from pynwb.device import Device
from pynwb.file import (
    Subject,
    SweepTable,
    SimultaneousRecordingsTable,
    SequentialRecordingsTable,
    RepetitionsTable,
    ExperimentalConditionsTable,
    IntracellularRecordingsTable,
)
from pynwb.ophys import ImagingPlane
from pynwb.ogen import OptogeneticStimulusSite
from pynwb.icephys import IntracellularElectrode


class NWBTyped:
    def __init__(self, nwb: Any):
        if isinstance(nwb, NWBTyped):
            nwb = nwb.nwb
        assert isinstance(nwb, pynwb.NWBFile)
        self.nwb: Any = nwb  # do this so we don't have to put "type ignore" everywhere

    @property
    def session_description(self) -> str:
        """A description of the session where this data was generated"""
        assert isinstance(self.nwb.session_description, str)
        return self.nwb.session_description

    @property
    def identifier(self) -> str:
        """A unique text identifier for the file"""
        assert isinstance(self.nwb.identifier, str)
        return str(self.nwb.identifier)

    @property
    def session_start_time(self) -> datetime:
        """The start date and time of the recording session"""
        assert isinstance(self.nwb.session_start_time, datetime)
        return self.nwb.session_start_time

    @property
    def file_create_date(self) -> datetime:
        """The date and time the file was created and subsequent modifications made"""
        # Docs say "ndarray or list or tuple or Dataset or Array or StrDataset or HDMFDataset or AbstractDataChunkIterator or datetime"
        # We're only going to handle a few of these cases
        if isinstance(self.nwb.file_create_date, datetime):
            return self.nwb.file_create_date
        elif isinstance(self.nwb.file_create_date, (list, tuple)):
            assert len(self.nwb.file_create_date) == 1
            dt = self.nwb.file_create_date[0]
            assert isinstance(dt, datetime)
            return dt
        elif isinstance(self.file_create_date, np.ndarray):
            assert len(self.nwb.file_create_date) == 1
            dt = self.nwb.file_create_date[0]
            assert isinstance(dt, datetime)
            return dt
        else:
            raise TypeError(
                f"Unexpected type for file_create_date: {type(self.nwb.file_create_date)}"
            )

    @property
    def timestamps_reference_time(self) -> datetime:
        """Date and time corresponding to time zero of all timestamps"""
        assert isinstance(self.nwb.timestamps_reference_time, datetime)
        return self.nwb.timestamps_reference_time

    @property
    def experimenter(self) -> List[str]:
        """Name of person who performed experiment"""
        # Docs say "tuple or list or str"
        if self.nwb.experimenter is None:
            return []
        elif isinstance(self.nwb.experimenter, str):
            return [self.nwb.experimenter]
        elif isinstance(self.nwb.experimenter, (list, tuple)):
            assert all(isinstance(e, str) for e in self.nwb.experimenter)
            return [str(e) for e in self.nwb.experimenter]
        else:
            raise TypeError(
                f"Unexpected type for experimenter: {type(self.nwb.experimenter)}"
            )

    @property
    def experiment_description(self) -> str:
        """General description of the experiment"""
        assert isinstance(self.nwb.experiment_description, str)
        return self.nwb.experiment_description

    @property
    def session_id(self) -> str:
        """Lab-specific ID for the session"""
        if self.nwb.session_id is None:
            return ""
        assert isinstance(self.nwb.session_id, str)
        return self.nwb.session_id

    @property
    def institution(self) -> str:
        """Institution(s) where experiment is performed"""
        if self.nwb.institution is None:
            return ""
        assert isinstance(self.nwb.institution, str)
        return self.nwb.institution

    @property
    def keywords(self) -> List[str]:
        """Terms to search over"""
        # Docs say ndarray or list or tuple or Dataset or Array or StrDataset or HDMFDataset or AbstractDataChunkIterator
        # We're only going to handle a few of these cases
        if self.nwb.keywords is None:
            return []
        elif isinstance(self.nwb.keywords, str):
            return [self.nwb.keywords]
        elif isinstance(self.nwb.keywords, (list, tuple)):
            assert all(isinstance(k, str) for k in self.nwb.keywords)
            return [str(k) for k in self.nwb.keywords]
        elif isinstance(self.nwb.keywords, np.ndarray):
            for k in self.nwb.keywords:
                assert isinstance(k, str)
            return [str(k) for k in self.nwb.keywords]
        elif isinstance(self.nwb.keywords, h5py.Dataset):
            for k in self.nwb.keywords:
                assert isinstance(k, str)
            return [str(k) for k in self.nwb.keywords]
        else:
            raise TypeError(f"Unexpected type for keywords: {type(self.nwb.keywords)}")

    @property
    def notes(self) -> str:
        """Notes about the experiment"""
        if self.nwb.notes is None:
            return ""
        assert isinstance(self.nwb.notes, str)
        return self.nwb.notes

    @property
    def pharmacology(self) -> str:
        """Description of drugs used, including how and when they were administered"""
        if self.nwb.pharmacology is None:
            return ""
        assert isinstance(self.nwb.pharmacology, str)
        return self.nwb.pharmacology

    @property
    def protocol(self) -> str:
        """Experimental protocol, if applicable"""
        if self.nwb.protocol is None:
            return ""
        assert isinstance(self.nwb.protocol, str)
        return self.nwb.protocol

    @property
    def related_publications(self) -> List[str]:
        """Publication information (PMID, DOI, URL, etc)"""
        # Docs say tuple or list or str
        if self.nwb.related_publications is None:
            return []
        if isinstance(self.nwb.related_publications, str):
            return [self.nwb.related_publications]
        elif isinstance(self.nwb.related_publications, (list, tuple)):
            assert all(isinstance(p, str) for p in self.nwb.related_publications)
            return [str(p) for p in self.nwb.related_publications]
        else:
            raise TypeError(
                f"Unexpected type for related_publications: {type(self.nwb.related_publications)}"
            )

    @property
    def slices(self) -> str:
        """Description of slices, including preparation information"""
        if self.nwb.slices is None:
            return ""
        assert isinstance(self.nwb.slices, str)
        return self.nwb.slices

    @property
    def source_script(self) -> str:
        """Script file used to create this NWB file"""
        if self.nwb.source_script is None:
            return ""
        assert isinstance(self.nwb.source_script, str)
        return self.nwb.source_script

    @property
    def source_script_file_name(self) -> str:
        """Name of the source_script file"""
        if self.nwb.source_script_file_name is None:
            return ""
        assert isinstance(self.nwb.source_script_file_name, str)
        return self.nwb.source_script_file_name

    @property
    def data_collection(self) -> str:
        """Notes about data collection and analysis"""
        if self.nwb.data_collection is None:
            return ""
        assert isinstance(self.nwb.data_collection, str)
        return self.nwb.data_collection

    @property
    def surgery(self) -> str:
        """Narrative description about surgery/surgeries"""
        if self.nwb.surgery is None:
            return ""
        assert isinstance(self.nwb.surgery, str)
        return self.nwb.surgery

    @property
    def virus(self) -> str:
        """Information about virus(es) used in experiments"""
        if self.nwb.virus is None:
            return ""
        assert isinstance(self.nwb.virus, str)
        return self.nwb.virus

    @property
    def stimulus_notes(self) -> str:
        """Notes about stimuli, such as how and where presented"""
        if self.nwb.stimulus_notes is None:
            return ""
        assert isinstance(self.nwb.stimulus_notes, str)
        return self.nwb.stimulus_notes

    @property
    def lab(self) -> str:
        """Lab where experiment was performed"""
        if self.nwb.lab is None:
            return ""
        assert isinstance(self.nwb.lab, str)
        return self.nwb.lab

    @property
    def acquisition(self) -> Dict[str, Any]:
        """Raw TimeSeries objects belonging to this NWBFile"""
        # docs say "list or tuple"
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.acquisition, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.acquisition.items():
                ret[key] = value
            return ret
        else:
            print(self.nwb.acquisition)
            raise TypeError(
                f"Unexpected type for acquisition: {type(self.nwb.acquisition)}"
            )

    @property
    def analysis(self) -> Dict[str, Any]:
        """Result of analysis"""
        # docs say "list or tuple"
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.analysis, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.analysis.items():
                ret[key] = value
            return ret
        else:
            raise TypeError(f"Unexpected type for analysis: {type(self.nwb.analysis)}")

    @property
    def stimulus(self) -> Dict[str, Any]:
        """Stimulus TimeSeries, DynamicTable, or NWBDataInterface objects"""
        # docs say "list or tuple"
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.stimulus, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.stimulus.items():
                ret[key] = value
            return ret
        else:
            raise TypeError(f"Unexpected type for stimulus: {type(self.nwb.stimulus)}")

    @property
    def stimulus_template(self) -> Dict[str, Any]:
        """Stimulus template TimeSeries objects"""
        # docs say "list or tuple"
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.stimulus_template, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.stimulus_template.items():
                ret[key] = value
            return ret
        else:
            raise TypeError(
                f"Unexpected type for stimulus_template: {type(self.nwb.stimulus_template)}"
            )

    @property
    def epochs(self) -> Optional[TimeIntervals]:
        """Epoch objects belonging to this NWBFile"""
        if self.nwb.epochs is None:
            return None
        assert isinstance(self.nwb.epochs, TimeIntervals)
        return self.nwb.epochs

    @property
    def trials(self) -> Optional[TimeIntervals]:
        """A table containing trial data"""
        if self.nwb.trials is None:
            return None
        assert isinstance(self.nwb.trials, TimeIntervals)
        return self.nwb.trials

    @property
    def invalid_times(self) -> Optional[TimeIntervals]:
        """A table containing times to be omitted from analysis"""
        if self.nwb.invalid_times is None:
            return None
        assert isinstance(self.nwb.invalid_times, TimeIntervals)
        return self.nwb.invalid_times

    @property
    def intervals(self) -> Dict[str, TimeIntervals]:
        """Any TimeIntervals tables storing time intervals"""
        # docs say list or tuple
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.intervals, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.intervals.items():
                assert isinstance(value, TimeIntervals)
                ret[key] = value
            return ret
        else:
            raise TypeError(
                f"Unexpected type for intervals: {type(self.nwb.intervals)}"
            )

    @property
    def units(self) -> Optional[Units]:
        """A table containing unit metadata"""
        if self.nwb.units is None:
            return None
        assert isinstance(self.nwb.units, Units)
        return self.nwb.units

    @property
    def processing(self) -> Dict[str, Any]:
        """ProcessingModule objects belonging to this NWBFile"""
        # docs say "list or tuple"
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.processing, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.processing.items():
                ret[key] = value
            return ret
        else:
            raise TypeError(
                f"Unexpected type for processing: {type(self.nwb.processing)}"
            )

    @property
    def lab_meta_data(self) -> Dict[str, Any]:
        """An extension that contains lab-specific meta-data"""
        # docs say list or tuple
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.lab_meta_data, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.lab_meta_data.items():
                ret[key] = value
            return ret
        else:
            raise TypeError(
                f"Unexpected type for lab_meta_data: {type(self.nwb.lab_meta_data)}"
            )

    @property
    def electrodes(self) -> Optional[DynamicTable]:
        """The ElectrodeTable that belongs to this NWBFile"""
        if self.nwb.electrodes is None:
            return None
        assert isinstance(self.nwb.electrodes, DynamicTable)
        return self.nwb.electrodes

    @property
    def electrode_groups(self) -> List[str]:
        """The ElectrodeGroups that belong to this NWBFile"""
        # docs say Iterable
        for eg in self.nwb.electrode_groups:
            assert isinstance(eg, str)
        return [eg for eg in self.nwb.electrode_groups]

    @property
    def sweep_table(self) -> Optional[SweepTable]:
        """The SweepTable that belongs to this NWBFile"""
        if self.nwb.sweep_table is None:
            return None
        assert isinstance(self.nwb.sweep_table, SweepTable)
        return self.nwb.sweep_table

    @property
    def imaging_planes(self) -> Dict[str, ImagingPlane]:
        """ImagingPlanes that belong to this NWBFile"""
        # docs say list or tuple
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.imaging_planes, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.imaging_planes.items():
                assert isinstance(value, ImagingPlane)
                ret[key] = value
            return ret
        else:
            raise TypeError(
                f"Unexpected type for imaging_planes: {type(self.nwb.imaging_planes)}"
            )

    @property
    def ogen_sites(self) -> Dict[str, OptogeneticStimulusSite]:
        """OptogeneticStimulusSites that belong to this NWBFile"""
        # docs say list or tuple
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.ogen_sites, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.ogen_sites.items():
                assert isinstance(value, OptogeneticStimulusSite)
                ret[key] = value
            return ret
        else:
            raise TypeError(
                f"Unexpected type for ogen_sites: {type(self.nwb.ogen_sites)}"
            )

    @property
    def devices(self) -> Dict[str, Device]:
        """Device objects belonging to this NWBFile"""
        # docs say list or tuple
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.devices, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.devices.items():
                assert isinstance(value, Device)
                ret[key] = value
            return ret
        else:
            raise TypeError(f"Unexpected type for devices: {type(self.nwb.devices)}")

    @property
    def subject(self) -> Subject:
        """Subject metadata"""
        assert isinstance(self.nwb.subject, Subject)
        return self.nwb.subject

    @property
    def scratch(self) -> Dict[str, Any]:
        """Scratch data"""
        # docs say list or tuple
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.scratch, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.scratch.items():
                ret[key] = value
            return ret
        else:
            raise TypeError(f"Unexpected type for scratch: {type(self.nwb.scratch)}")

    @property
    def icephys_electrodes(self) -> Dict[str, IntracellularElectrode]:
        """IntracellularElectrodes that belong to this NWBFile"""
        # docs say list or tuple
        # but found hdmf.utils.LabelledDict in the wild
        if isinstance(self.nwb.icephys_electrodes, hdmf.utils.LabelledDict):
            ret = {}
            for key, value in self.nwb.icephys_electrodes.items():
                assert isinstance(value, IntracellularElectrode)
                ret[key] = value
            return ret
        else:
            raise TypeError(
                f"Unexpected type for icephys_electrodes: {type(self.nwb.icephys_electrodes)}"
            )

    @property
    def intracellular_recordings(self) -> Optional[IntracellularRecordingsTable]:
        """The IntracellularRecordingsTable table that belongs to this NWBFile"""
        if self.nwb.intracellular_recordings is None:
            return None
        assert isinstance(
            self.nwb.intracellular_recordings, IntracellularRecordingsTable
        )
        return self.nwb.intracellular_recordings

    @property
    def icephys_simultaneous_recordings(self) -> Optional[SimultaneousRecordingsTable]:
        """The SimultaneousRecordingsTable table that belongs to this NWBFile"""
        if self.nwb.icephys_simultaneous_recordings is None:
            return None
        assert isinstance(
            self.nwb.icephys_simultaneous_recordings, SimultaneousRecordingsTable
        )
        return self.nwb.icephys_simultaneous_recordings

    @property
    def icephys_sequential_recordings(self) -> Optional[SequentialRecordingsTable]:
        """The SequentialRecordingsTable table that belongs to this NWBFile"""
        if self.nwb.icephys_sequential_recordings is None:
            return None
        assert isinstance(
            self.nwb.icephys_sequential_recordings, SequentialRecordingsTable
        )
        return self.nwb.icephys_sequential_recordings

    @property
    def icephys_repetitions(self) -> Optional[RepetitionsTable]:
        """The RepetitionsTable table that belongs to this NWBFile"""
        if self.nwb.icephys_repetitions is None:
            return None
        assert isinstance(self.nwb.icephys_repetitions, RepetitionsTable)
        return self.nwb.icephys_repetitions

    @property
    def icephys_experimental_conditions(self) -> Optional[ExperimentalConditionsTable]:
        """The ExperimentalConditionsTable table that belongs to this NWBFile"""
        if self.nwb.icephys_experimental_conditions is None:
            return None
        assert isinstance(
            self.nwb.icephys_experimental_conditions, ExperimentalConditionsTable
        )
        return self.nwb.icephys_experimental_conditions

    def __str__(self) -> str:
        s = ""
        s += f"Session description: {self.session_description}\n"
        s += f"Identifier: {self.identifier}\n"
        s += f"Session start time: {self.session_start_time}\n"
        s += f"File create date: {self.file_create_date}\n"
        s += f"Timestamps reference time: {self.timestamps_reference_time}\n"
        s += f"Experimenter: {self.experimenter}\n"
        s += f"Experiment description: {self.experiment_description}\n"
        s += f"Session ID: {self.session_id}\n"
        s += f"Institution: {self.institution}\n"
        s += f"Keywords: {self.keywords}\n"
        if self.notes:
            s += f"Notes: {self.notes}\n"
        if self.pharmacology:
            s += f"Pharmacology: {self.pharmacology}\n"
        if self.protocol:
            s += f"Protocol: {self.protocol}\n"
        if self.related_publications:
            s += f"Related publications: {self.related_publications}\n"
        if self.slices:
            s += f"Slices: {self.slices}\n"
        if self.source_script:
            s += f"Source script: {self.source_script}\n"
        if self.source_script_file_name:
            s += f"Source script file name: {self.source_script_file_name}\n"
        if self.data_collection:
            s += f"Data collection: {self.data_collection}\n"
        if self.surgery:
            s += f"Surgery: {self.surgery}\n"
        if self.virus:
            s += f"Virus: {self.virus}\n"
        if self.stimulus_notes:
            s += f"Stimulus notes: {self.stimulus_notes}\n"
        if self.lab:
            s += f"Lab: {self.lab}\n"
        if self.acquisition:
            s += "Acquisition:\n"
            for k in self.acquisition.keys():
                s += f"  {k}:\n"
                s += indent(str(self.acquisition[k]), 4) + "\n"
        if self.analysis:
            s += f"Analysis:\n"
            for k in self.analysis.keys():
                s += f"  {k}:\n"
                s += indent(str(self.analysis[k]), 4) + "\n"
        if self.stimulus:
            s += f"Stimulus:\n"
            for k in self.stimulus.keys():
                s += f"  {k}:\n"
                s += indent(str(self.stimulus[k]), 4) + "\n"
        if self.stimulus_template:
            s += f"Stimulus template:\n"
            for k in self.stimulus_template.keys():
                s += f"  {k}:\n"
                s += indent(str(self.stimulus_template[k]), 4) + "\n"
        if self.epochs:
            s += f"Epochs:\n"
            s += indent(str(self.epochs), 4) + "\n"
        if self.trials:
            s += f"Trials:\n"
            s += indent(str(self.trials), 4) + "\n"
        if self.invalid_times:
            s += f"Invalid times:\n"
            s += indent(str(self.invalid_times), 4) + "\n"
        if self.intervals:
            s += f"Intervals:\n"
            for k in self.intervals.keys():
                s += f"  {k}:\n"
                s += indent(str(self.intervals[k]), 4) + "\n"
        if self.units:
            s += f"Units:\n"
            s += indent(str(self.units), 4) + "\n"
        if self.processing:
            s += f"Processing:\n"
            for k in self.processing.keys():
                s += f"  {k}:\n"
                s += indent(str(self.processing[k]), 4) + "\n"
        if self.lab_meta_data:
            s += f"Lab meta data:\n"
            for k in self.lab_meta_data.keys():
                s += f"  {k}:\n"
                s += indent(str(self.lab_meta_data[k]), 4) + "\n"
        if self.electrodes:
            s += f"Electrodes:\n"
            s += indent(str(self.electrodes), 4) + "\n"
        if self.electrode_groups:
            s += f"Electrode groups:\n"
            for eg in self.electrode_groups:
                s += indent(str(eg), 4) + "\n"
        if self.sweep_table:
            s += f"Sweep table:\n"
            s += indent(str(self.sweep_table), 4) + "\n"
        if self.imaging_planes:
            s += f"Imaging planes:\n"
            for k in self.imaging_planes.keys():
                s += f"  {k}:\n"
                s += indent(str(self.imaging_planes[k]), 4) + "\n"
        if self.ogen_sites:
            s += f"Ogen sites:\n"
            for k in self.ogen_sites.keys():
                s += f"  {k}:\n"
                s += indent(str(self.ogen_sites[k]), 4) + "\n"
        if self.devices:
            s += f"Devices:\n"
            for k in self.devices.keys():
                s += f"  {k}:\n"
                s += indent(str(self.devices[k]), 4) + "\n"
        if self.subject:
            s += f"Subject:\n"
            s += indent(str(self.subject), 4) + "\n"
        if self.scratch:
            s += f"Scratch:\n"
            for k in self.scratch.keys():
                s += f"  {k}:\n"
                s += indent(str(self.scratch[k]), 4) + "\n"
        if self.icephys_electrodes:
            s += f"Icephys electrodes:\n"
            for k in self.icephys_electrodes.keys():
                s += f"  {k}:\n"
                s += indent(str(self.icephys_electrodes[k]), 4) + "\n"
        if self.intracellular_recordings:
            s += f"Intracellular recordings:\n"
            s += indent(str(self.intracellular_recordings), 4) + "\n"
        if self.icephys_simultaneous_recordings:
            s += f"Icephys simultaneous recordings:\n"
            s += indent(str(self.icephys_simultaneous_recordings), 4) + "\n"
        if self.icephys_sequential_recordings:
            s += f"Icephys sequential recordings:\n"
            s += indent(str(self.icephys_sequential_recordings), 4) + "\n"
        if self.icephys_repetitions:
            s += f"Icephys repetitions:\n"
            s += indent(str(self.icephys_repetitions), 4) + "\n"
        if self.icephys_experimental_conditions:
            s += f"Icephys experimental conditions:\n"
            s += indent(str(self.icephys_experimental_conditions), 4) + "\n"
        return s


def indent(s: str, n: int) -> str:
    return "\n".join(" " * n + line for line in s.split("\n"))


