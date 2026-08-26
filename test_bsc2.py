from bsimvis.app.services.graph_service import graph_service
def test_bsc2():
    print("Testing BSC2 packing/unpacking...")
    
    # 0.9 => 9000
    u = 12345
    v = 67890
    score = 0.9521
    
    packed = graph_service.pack_bsc2_edge(u, v, score)
    u_out, v_out, score_out = graph_service.unpack_bsc2_edge(packed)
    
    assert u == u_out
    assert v == v_out
    assert abs(int(score * 10000) - score_out) < 1
    
    print("BSC2 round-trip SUCCESS!")
    
if __name__ == "__main__":
    test_bsc2()
