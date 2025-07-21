# import pyscf
# import pyscf.cc
# import pyscf.mcscf
import ffsim
import numpy as np
import csv

# from qiskit_ibm_runtime import QiskitRuntimeService
# from qiskit_ibm_runtime import SamplerV2 as SamplerV2

from prefect import flow, task
from prefect_qiskit import QuantumRuntime, runtime
from prefect.variables import Variable
from prefect.logging import get_run_logger
from prefect_qiskit.vendors.ibm_quantum import IBMQuantumCredentials
from prefect_qiskit import QuantumRuntime
from qiskit.circuit import QuantumCircuit
from qiskit.transpiler import generate_preset_pass_manager


import molecule_build as mb
import loader
import create_ansatz as ca
import ansatz_optimiser 
import recovery_solver as rs

from dataclasses import dataclass

from qiskit_ibm_runtime import QiskitRuntimeService


@dataclass
class MoleculeInput:
    atom: any
    basis: str
    symmetry: bool
    spin_sq: int
    charge: int
    n_frozen: int

@dataclass
class MoleculeData:
    mo: any
    hcore: any
    nuclear_repulsion_energy: float
    num_orbitals: int
    active_space: any
    eri: any
    scf:any
    num_elec_a:int
    num_elec_b: int

@task
def load_molecule_task(filepath: str) -> MoleculeInput:
    atom, basis, symmetry, spin_sq, charge, n_frozen = loader.load_molecule(filepath)
    return MoleculeInput(atom, basis, symmetry, spin_sq, charge, n_frozen or 0)

@task
def compute_integrals_task(mol_input: MoleculeInput) -> MoleculeData:
    mol, mo, hcore, enuc, n_orb, active_space, eri, scf, num_elec_a, num_elec_b = mb.mol_integrals(
        mol_input.atom,
        mol_input.basis,
        mol_input.symmetry,
        mol_input.spin_sq,
        mol_input.charge,
        mol_input.n_frozen,
    )
    return MoleculeData(mo, hcore, enuc, n_orb, active_space, eri, scf, num_elec_a, num_elec_b) , mol

@task
def init_ansatz(backend,n_reps,num_orbitals,optimisation_level,mol,active_space,scf,num_elec_a,num_elec_b):
    circuit = ca.create_ansatz(scf,num_orbitals,mol,active_space,num_elec_a,num_elec_b,n_reps)
    isa_circuit = ansatz_optimiser.optimiser(circuit,num_orbitals,backend,optimisation_level)
    return isa_circuit

@task
def bitstring(pub_result):
    counts  = pub_result.data.meas.get_counts()
    with open("counts.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["bitstring", "count"])
        for bitstring, count in counts.items():
            writer.writerow([bitstring, count])

@task
def run_sqd_task(mol_data, pub_result, nelec):
    logger = get_run_logger()
    logger.info("Starting SQD computation...")

    sqd_options = rs.set_sqd_options()
    callback = rs.define_sqd_callback(mol_data.nuclear_repulsion_energy)
    sqd_result = rs.compute_sqd_result(
        mol_data.hcore,
        mol_data.eri,
        pub_result.data.meas,
        mol_data.num_orbitals,
        nelec,
        sqd_options,
        callback
    )

    logger.info("SQD computation completed.")
    return sqd_result
   

@flow
def sqd(filepath):
    
    logger = get_run_logger()

    runtime = QuantumRuntime.load("default-runtime")
    options = Variable.get("sampler_options")
    
    quantum_credentials = IBMQuantumCredentials.load("my-ibm-client")

    mol_input = load_molecule_task(filepath)
    mol_data , mol = compute_integrals_task(mol_input)
    
    n_reps = int(input("Enter repetitions for UCJ operator multiplication (preferably 1 ) : "))
    optimisation_level = int(input("Enter optimisation level ( 1 :least dense to 3 : most dense) : "))
    
    
    resource_name = runtime.resource_name
    token = quantum_credentials.api_key.get_secret_value()
    service = QiskitRuntimeService(channel = "ibm_quantum_platform" , token = token )
    backend = service.backend(resource_name)

    isa_crc = init_ansatz(backend,n_reps, mol_data.num_orbitals, optimisation_level,mol,mol_data.active_space, mol_data.scf, mol_data.num_elec_a,mol_data.num_elec_b)
    job = runtime.sampler([isa_crc], options=options)
    primitive_result = job
    pub_result = primitive_result[0]

    bitstring(pub_result)
    
    nelec = (mol_data.num_elec_a, mol_data.num_elec_b)
    sqd_result = run_sqd_task(mol_data, pub_result, nelec)

    try:
        with open("result.txt", "w") as f:
            f.write("Final SQD Result:\n")
            f.write(f"Energy: {sqd_result.energy + mol_data.nuclear_repulsion_energy}\n")
            f.write(f"Full Result Object: {str(sqd_result)}\n")
        logger.info("SQD result written to result.txt")
    except Exception as e:
        logger.error(f"Failed to write result.txt: {e}")


if __name__ == "__main__":
    sqd(filepath =  "/home/sxm/Diagonaliser/molecule.txt")