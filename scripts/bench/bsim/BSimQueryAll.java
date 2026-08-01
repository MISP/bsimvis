//Queries every function of the current program against a BSim database and writes
//the matches to JSON. Headless: used by scripts/bench/bsim_baseline.py to score
//Ghidra's own BSim retrieval on the same corpus BSimVis is measured on.
//
//This is benchmark-only. Nothing in BSimVis depends on a BSim database, and this
//script is never loaded by the application -- it runs inside Ghidra's own
//analyzeHeadless, from the Ghidra install that is already vendored in bin/.
//
//args: <bsimURL> <outputJson> [maxMatches] [simThreshold] [sigThreshold]
//@category BSim
import java.io.PrintWriter;
import java.util.Iterator;

import ghidra.app.script.GhidraScript;
import ghidra.features.bsim.query.BSimClientFactory;
import ghidra.features.bsim.query.FunctionDatabase;
import ghidra.features.bsim.query.GenSignatures;
import ghidra.features.bsim.query.description.FunctionDescription;
import ghidra.features.bsim.query.protocol.QueryNearest;
import ghidra.features.bsim.query.protocol.ResponseNearest;
import ghidra.features.bsim.query.protocol.SimilarityNote;
import ghidra.features.bsim.query.protocol.SimilarityResult;
import ghidra.program.model.listing.Function;

public class BSimQueryAll extends GhidraScript {

	@Override
	protected void run() throws Exception {
		String[] args = getScriptArgs();
		if (args.length < 2) {
			println("usage: BSimQueryAll <bsimURL> <outputJson> [max] [sim] [sig]");
			return;
		}
		String dbUrl = args[0];
		String outPath = args[1];
		int max = args.length > 2 ? Integer.parseInt(args[2]) : 10;
		double simThresh = args.length > 3 ? Double.parseDouble(args[3]) : 0.0;
		double sigThresh = args.length > 4 ? Double.parseDouble(args[4]) : 0.0;

		FunctionDatabase database = BSimClientFactory.buildClient(
			BSimClientFactory.deriveBSimURL(dbUrl), false);
		if (!database.initialize()) {
			println("BSim connect failed: " + database.getLastError().message);
			return;
		}

		GenSignatures gensig = new GenSignatures(false);
		gensig.setVectorFactory(database.getLSHVectorFactory());
		gensig.openProgram(currentProgram, null, null, null, null, null);
		int scanned = 0;
		for (Function f : currentProgram.getFunctionManager().getFunctions(true)) {
			if (f.isThunk()) {
				continue;
			}
			gensig.scanFunction(f);
			scanned++;
		}

		QueryNearest query = new QueryNearest();
		query.manage = gensig.getDescriptionManager();
		query.max = max;
		query.thresh = simThresh;
		query.signifthresh = sigThresh;

		long t0 = System.currentTimeMillis();
		ResponseNearest response = (ResponseNearest) database.query(query);
		long elapsed = System.currentTimeMillis() - t0;
		if (response == null) {
			println("query failed: " + database.getLastError().message);
			gensig.dispose();
			database.close();
			return;
		}

		PrintWriter out = new PrintWriter(outPath);
		out.println("{");
		out.println(" \"program\": \"" + esc(currentProgram.getName()) + "\",");
		out.println(" \"queried_functions\": " + scanned + ",");
		out.println(" \"query_millis\": " + elapsed + ",");
		out.println(" \"results\": [");
		boolean firstResult = true;
		Iterator<SimilarityResult> resIter = response.result.iterator();
		while (resIter.hasNext()) {
			SimilarityResult sim = resIter.next();
			if (!firstResult) {
				out.println(",");
			}
			firstResult = false;
			out.print("  {\"function\": \"" + esc(sim.getBase().getFunctionName()) + "\", \"matches\": [");
			boolean firstMatch = true;
			Iterator<SimilarityNote> noteIter = sim.iterator();
			while (noteIter.hasNext()) {
				SimilarityNote note = noteIter.next();
				FunctionDescription fdesc = note.getFunctionDescription();
				if (!firstMatch) {
					out.print(", ");
				}
				firstMatch = false;
				out.print("{\"exe\": \"" + esc(fdesc.getExecutableRecord().getNameExec())
					+ "\", \"name\": \"" + esc(fdesc.getFunctionName())
					+ "\", \"similarity\": " + note.getSimilarity()
					+ ", \"significance\": " + note.getSignificance() + "}");
			}
			out.print("]}");
		}
		out.println();
		out.println(" ]");
		out.println("}");
		out.close();
		println("wrote " + outPath + " (" + scanned + " functions, " + elapsed + " ms)");

		gensig.dispose();
		database.close();
	}

	private static String esc(String s) {
		return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
	}
}
