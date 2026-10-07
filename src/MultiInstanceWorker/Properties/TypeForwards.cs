using System.Runtime.CompilerServices;
using MultiInstanceWorker;

// These types lived in this assembly up to 3.0.0 and moved to MultiInstanceWorker.Abstractions in
// 3.1.0. The forwards keep anything compiled against an older MultiInstanceWorker.dll resolving
// them without a recompile.
[assembly: TypeForwardedTo(typeof(ActiveInstance))]
[assembly: TypeForwardedTo(typeof(IDrainableService))]
[assembly: TypeForwardedTo(typeof(IInstanceIdentityProvider))]
[assembly: TypeForwardedTo(typeof(IInstanceRegistry))]
[assembly: TypeForwardedTo(typeof(ILeaseManager))]
[assembly: TypeForwardedTo(typeof(IWorkloadAssigner))]
[assembly: TypeForwardedTo(typeof(IWorkloadStatusStore))]
[assembly: TypeForwardedTo(typeof(WorkloadStatus))]
[assembly: TypeForwardedTo(typeof(WorkloadStatusRecord))]
