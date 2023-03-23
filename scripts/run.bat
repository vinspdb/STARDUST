IF "%1"=="SPL" (
	IF "%2"=="True" (
		java -cp "lib/*" au.edu.unimelb.services.ServiceProvider "SMD" 0.1 0.4 "false" %3 %4
	) ELSE (
		java -cp "lib/*" au.edu.unimelb.services.ServiceProvider "SMD" 0.1 0.0 "false" %3 %4
	)
) ELSE (
	set USE_FILTER=%2
	set IMPORTLOG=%3
	set EXPORTMODEL=%4.pnml
	java -da -Xmx8G -classpath "lib/*" -Djava.library.path=./lib -Djava.util.Arrays.useLegacyMergeSort=true org.processmining.contexts.cli.CLI -f scripts/ILP.txt
)
