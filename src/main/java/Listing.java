public class Listing {
    private String q_id;
    private String iter;
    private String docno;
    private int rank;
    private float sim;
    private String run_id;

    public Listing(String q_id, String iter, String docno, int rank, float sim, String run_id) {
        this.q_id = q_id;
        this.iter = iter;
        this.docno = docno;
        this.rank = rank;
        this.sim = sim;
        this.run_id = run_id;
    }

    @Override
    public String toString() {
        return q_id +
                "\t" + iter +
                "\t" + docno +
                "\t" + rank +
                "\t" + sim +
                "\t" + run_id;
    }

    public String getQ_id() {
        return q_id;
    }

    public String getIter() {
        return iter;
    }

    public String getDocno() {
        return docno;
    }

    public int getRank() {
        return rank;
    }

    public float getSim() {
        return sim;
    }

    public String getRun_id() {
        return run_id;
    }
}
