var Or = Object.defineProperty;
var Mr = (e, t, n) => t in e ? Or(e, t, { enumerable: !0, configurable: !0, writable: !0, value: n }) : e[t] = n;
var E = (e, t, n) => Mr(e, typeof t != "symbol" ? t + "" : t, n);
function kr(e) {
  const t = +this._x.call(null, e), n = +this._y.call(null, e);
  return Gi(this.cover(t, n), t, n, e);
}
function Gi(e, t, n, i) {
  if (isNaN(t) || isNaN(n)) return e;
  var r, o = e._root, a = { data: i }, l = e._x0, h = e._y0, s = e._x1, f = e._y1, w, m, p, A, y, _, v, S;
  if (!o) return e._root = a, e;
  for (; o.length; )
    if ((y = t >= (w = (l + s) / 2)) ? l = w : s = w, (_ = n >= (m = (h + f) / 2)) ? h = m : f = m, r = o, !(o = o[v = _ << 1 | y])) return r[v] = a, e;
  if (p = +e._x.call(null, o.data), A = +e._y.call(null, o.data), t === p && n === A) return a.next = o, r ? r[v] = a : e._root = a, e;
  do
    r = r ? r[v] = new Array(4) : e._root = new Array(4), (y = t >= (w = (l + s) / 2)) ? l = w : s = w, (_ = n >= (m = (h + f) / 2)) ? h = m : f = m;
  while ((v = _ << 1 | y) === (S = (A >= m) << 1 | p >= w));
  return r[S] = o, r[v] = a, e;
}
function Lr(e) {
  var t, n, i = e.length, r, o, a = new Array(i), l = new Array(i), h = 1 / 0, s = 1 / 0, f = -1 / 0, w = -1 / 0;
  for (n = 0; n < i; ++n)
    isNaN(r = +this._x.call(null, t = e[n])) || isNaN(o = +this._y.call(null, t)) || (a[n] = r, l[n] = o, r < h && (h = r), r > f && (f = r), o < s && (s = o), o > w && (w = o));
  if (h > f || s > w) return this;
  for (this.cover(h, s).cover(f, w), n = 0; n < i; ++n)
    Gi(this, a[n], l[n], e[n]);
  return this;
}
function Fr(e, t) {
  if (isNaN(e = +e) || isNaN(t = +t)) return this;
  var n = this._x0, i = this._y0, r = this._x1, o = this._y1;
  if (isNaN(n))
    r = (n = Math.floor(e)) + 1, o = (i = Math.floor(t)) + 1;
  else {
    for (var a = r - n || 1, l = this._root, h, s; n > e || e >= r || i > t || t >= o; )
      switch (s = (t < i) << 1 | e < n, h = new Array(4), h[s] = l, l = h, a *= 2, s) {
        case 0:
          r = n + a, o = i + a;
          break;
        case 1:
          n = r - a, o = i + a;
          break;
        case 2:
          r = n + a, i = o - a;
          break;
        case 3:
          n = r - a, i = o - a;
          break;
      }
    this._root && this._root.length && (this._root = l);
  }
  return this._x0 = n, this._y0 = i, this._x1 = r, this._y1 = o, this;
}
function Pr() {
  var e = [];
  return this.visit(function(t) {
    if (!t.length) do
      e.push(t.data);
    while (t = t.next);
  }), e;
}
function zr(e) {
  return arguments.length ? this.cover(+e[0][0], +e[0][1]).cover(+e[1][0], +e[1][1]) : isNaN(this._x0) ? void 0 : [[this._x0, this._y0], [this._x1, this._y1]];
}
function ct(e, t, n, i, r) {
  this.node = e, this.x0 = t, this.y0 = n, this.x1 = i, this.y1 = r;
}
function Br(e, t, n) {
  var i, r = this._x0, o = this._y0, a, l, h, s, f = this._x1, w = this._y1, m = [], p = this._root, A, y;
  for (p && m.push(new ct(p, r, o, f, w)), n == null ? n = 1 / 0 : (r = e - n, o = t - n, f = e + n, w = t + n, n *= n); A = m.pop(); )
    if (!(!(p = A.node) || (a = A.x0) > f || (l = A.y0) > w || (h = A.x1) < r || (s = A.y1) < o))
      if (p.length) {
        var _ = (a + h) / 2, v = (l + s) / 2;
        m.push(
          new ct(p[3], _, v, h, s),
          new ct(p[2], a, v, _, s),
          new ct(p[1], _, l, h, v),
          new ct(p[0], a, l, _, v)
        ), (y = (t >= v) << 1 | e >= _) && (A = m[m.length - 1], m[m.length - 1] = m[m.length - 1 - y], m[m.length - 1 - y] = A);
      } else {
        var S = e - +this._x.call(null, p.data), x = t - +this._y.call(null, p.data), g = S * S + x * x;
        if (g < n) {
          var I = Math.sqrt(n = g);
          r = e - I, o = t - I, f = e + I, w = t + I, i = p.data;
        }
      }
  return i;
}
function Ur(e) {
  if (isNaN(f = +this._x.call(null, e)) || isNaN(w = +this._y.call(null, e))) return this;
  var t, n = this._root, i, r, o, a = this._x0, l = this._y0, h = this._x1, s = this._y1, f, w, m, p, A, y, _, v;
  if (!n) return this;
  if (n.length) for (; ; ) {
    if ((A = f >= (m = (a + h) / 2)) ? a = m : h = m, (y = w >= (p = (l + s) / 2)) ? l = p : s = p, t = n, !(n = n[_ = y << 1 | A])) return this;
    if (!n.length) break;
    (t[_ + 1 & 3] || t[_ + 2 & 3] || t[_ + 3 & 3]) && (i = t, v = _);
  }
  for (; n.data !== e; ) if (r = n, !(n = n.next)) return this;
  return (o = n.next) && delete n.next, r ? (o ? r.next = o : delete r.next, this) : t ? (o ? t[_] = o : delete t[_], (n = t[0] || t[1] || t[2] || t[3]) && n === (t[3] || t[2] || t[1] || t[0]) && !n.length && (i ? i[v] = n : this._root = n), this) : (this._root = o, this);
}
function jr(e) {
  for (var t = 0, n = e.length; t < n; ++t) this.remove(e[t]);
  return this;
}
function Gr() {
  return this._root;
}
function Hr() {
  var e = 0;
  return this.visit(function(t) {
    if (!t.length) do
      ++e;
    while (t = t.next);
  }), e;
}
function Wr(e) {
  var t = [], n, i = this._root, r, o, a, l, h;
  for (i && t.push(new ct(i, this._x0, this._y0, this._x1, this._y1)); n = t.pop(); )
    if (!e(i = n.node, o = n.x0, a = n.y0, l = n.x1, h = n.y1) && i.length) {
      var s = (o + l) / 2, f = (a + h) / 2;
      (r = i[3]) && t.push(new ct(r, s, f, l, h)), (r = i[2]) && t.push(new ct(r, o, f, s, h)), (r = i[1]) && t.push(new ct(r, s, a, l, f)), (r = i[0]) && t.push(new ct(r, o, a, s, f));
    }
  return this;
}
function Vr(e) {
  var t = [], n = [], i;
  for (this._root && t.push(new ct(this._root, this._x0, this._y0, this._x1, this._y1)); i = t.pop(); ) {
    var r = i.node;
    if (r.length) {
      var o, a = i.x0, l = i.y0, h = i.x1, s = i.y1, f = (a + h) / 2, w = (l + s) / 2;
      (o = r[0]) && t.push(new ct(o, a, l, f, w)), (o = r[1]) && t.push(new ct(o, f, l, h, w)), (o = r[2]) && t.push(new ct(o, a, w, f, s)), (o = r[3]) && t.push(new ct(o, f, w, h, s));
    }
    n.push(i);
  }
  for (; i = n.pop(); )
    e(i.node, i.x0, i.y0, i.x1, i.y1);
  return this;
}
function Yr(e) {
  return e[0];
}
function $r(e) {
  return arguments.length ? (this._x = e, this) : this._x;
}
function qr(e) {
  return e[1];
}
function Kr(e) {
  return arguments.length ? (this._y = e, this) : this._y;
}
function ii(e, t, n) {
  var i = new ri(t ?? Yr, n ?? qr, NaN, NaN, NaN, NaN);
  return e == null ? i : i.addAll(e);
}
function ri(e, t, n, i, r, o) {
  this._x = e, this._y = t, this._x0 = n, this._y0 = i, this._x1 = r, this._y1 = o, this._root = void 0;
}
function gi(e) {
  for (var t = { data: e.data }, n = t; e = e.next; ) n = n.next = { data: e.data };
  return t;
}
var ht = ii.prototype = ri.prototype;
ht.copy = function() {
  var e = new ri(this._x, this._y, this._x0, this._y0, this._x1, this._y1), t = this._root, n, i;
  if (!t) return e;
  if (!t.length) return e._root = gi(t), e;
  for (n = [{ source: t, target: e._root = new Array(4) }]; t = n.pop(); )
    for (var r = 0; r < 4; ++r)
      (i = t.source[r]) && (i.length ? n.push({ source: i, target: t.target[r] = new Array(4) }) : t.target[r] = gi(i));
  return e;
};
ht.add = kr;
ht.addAll = Lr;
ht.cover = Fr;
ht.data = Pr;
ht.extent = zr;
ht.find = Br;
ht.remove = Ur;
ht.removeAll = jr;
ht.root = Gr;
ht.size = Hr;
ht.visit = Wr;
ht.visitAfter = Vr;
ht.x = $r;
ht.y = Kr;
function nt(e) {
  return function() {
    return e;
  };
}
function Zt(e) {
  return (e() - 0.5) * 1e-6;
}
function Xr(e) {
  return e.x + e.vx;
}
function Zr(e) {
  return e.y + e.vy;
}
function Qr(e) {
  var t, n, i, r = 1, o = 1;
  typeof e != "function" && (e = nt(e == null ? 1 : +e));
  function a() {
    for (var s, f = t.length, w, m, p, A, y, _, v = 0; v < o; ++v)
      for (w = ii(t, Xr, Zr).visitAfter(l), s = 0; s < f; ++s)
        m = t[s], y = n[m.index], _ = y * y, p = m.x + m.vx, A = m.y + m.vy, w.visit(S);
    function S(x, g, I, C, L) {
      var F = x.data, Y = x.r, j = y + Y;
      if (F) {
        if (F.index > m.index) {
          var Q = p - F.x - F.vx, at = A - F.y - F.vy, ot = Q * Q + at * at;
          ot < j * j && (Q === 0 && (Q = Zt(i), ot += Q * Q), at === 0 && (at = Zt(i), ot += at * at), ot = (j - (ot = Math.sqrt(ot))) / ot * r, m.vx += (Q *= ot) * (j = (Y *= Y) / (_ + Y)), m.vy += (at *= ot) * j, F.vx -= Q * (j = 1 - j), F.vy -= at * j);
        }
        return;
      }
      return g > p + j || C < p - j || I > A + j || L < A - j;
    }
  }
  function l(s) {
    if (s.data) return s.r = n[s.data.index];
    for (var f = s.r = 0; f < 4; ++f)
      s[f] && s[f].r > s.r && (s.r = s[f].r);
  }
  function h() {
    if (t) {
      var s, f = t.length, w;
      for (n = new Array(f), s = 0; s < f; ++s) w = t[s], n[w.index] = +e(w, s, t);
    }
  }
  return a.initialize = function(s, f) {
    t = s, i = f, h();
  }, a.iterations = function(s) {
    return arguments.length ? (o = +s, a) : o;
  }, a.strength = function(s) {
    return arguments.length ? (r = +s, a) : r;
  }, a.radius = function(s) {
    return arguments.length ? (e = typeof s == "function" ? s : nt(+s), h(), a) : e;
  }, a;
}
function Jr(e) {
  return e.index;
}
function mi(e, t) {
  var n = e.get(t);
  if (!n) throw new Error("node not found: " + t);
  return n;
}
function to(e) {
  var t = Jr, n = w, i, r = nt(30), o, a, l, h, s, f = 1;
  e == null && (e = []);
  function w(_) {
    return 1 / Math.min(l[_.source.index], l[_.target.index]);
  }
  function m(_) {
    for (var v = 0, S = e.length; v < f; ++v)
      for (var x = 0, g, I, C, L, F, Y, j; x < S; ++x)
        g = e[x], I = g.source, C = g.target, L = C.x + C.vx - I.x - I.vx || Zt(s), F = C.y + C.vy - I.y - I.vy || Zt(s), Y = Math.sqrt(L * L + F * F), Y = (Y - o[x]) / Y * _ * i[x], L *= Y, F *= Y, C.vx -= L * (j = h[x]), C.vy -= F * j, I.vx += L * (j = 1 - j), I.vy += F * j;
  }
  function p() {
    if (a) {
      var _, v = a.length, S = e.length, x = new Map(a.map((I, C) => [t(I, C, a), I])), g;
      for (_ = 0, l = new Array(v); _ < S; ++_)
        g = e[_], g.index = _, typeof g.source != "object" && (g.source = mi(x, g.source)), typeof g.target != "object" && (g.target = mi(x, g.target)), l[g.source.index] = (l[g.source.index] || 0) + 1, l[g.target.index] = (l[g.target.index] || 0) + 1;
      for (_ = 0, h = new Array(S); _ < S; ++_)
        g = e[_], h[_] = l[g.source.index] / (l[g.source.index] + l[g.target.index]);
      i = new Array(S), A(), o = new Array(S), y();
    }
  }
  function A() {
    if (a)
      for (var _ = 0, v = e.length; _ < v; ++_)
        i[_] = +n(e[_], _, e);
  }
  function y() {
    if (a)
      for (var _ = 0, v = e.length; _ < v; ++_)
        o[_] = +r(e[_], _, e);
  }
  return m.initialize = function(_, v) {
    a = _, s = v, p();
  }, m.links = function(_) {
    return arguments.length ? (e = _, p(), m) : e;
  }, m.id = function(_) {
    return arguments.length ? (t = _, m) : t;
  }, m.iterations = function(_) {
    return arguments.length ? (f = +_, m) : f;
  }, m.strength = function(_) {
    return arguments.length ? (n = typeof _ == "function" ? _ : nt(+_), A(), m) : n;
  }, m.distance = function(_) {
    return arguments.length ? (r = typeof _ == "function" ? _ : nt(+_), y(), m) : r;
  }, m;
}
var eo = { value: () => {
} };
function oi() {
  for (var e = 0, t = arguments.length, n = {}, i; e < t; ++e) {
    if (!(i = arguments[e] + "") || i in n || /[\s.]/.test(i)) throw new Error("illegal type: " + i);
    n[i] = [];
  }
  return new fn(n);
}
function fn(e) {
  this._ = e;
}
function no(e, t) {
  return e.trim().split(/^|\s+/).map(function(n) {
    var i = "", r = n.indexOf(".");
    if (r >= 0 && (i = n.slice(r + 1), n = n.slice(0, r)), n && !t.hasOwnProperty(n)) throw new Error("unknown type: " + n);
    return { type: n, name: i };
  });
}
fn.prototype = oi.prototype = {
  constructor: fn,
  on: function(e, t) {
    var n = this._, i = no(e + "", n), r, o = -1, a = i.length;
    if (arguments.length < 2) {
      for (; ++o < a; ) if ((r = (e = i[o]).type) && (r = io(n[r], e.name))) return r;
      return;
    }
    if (t != null && typeof t != "function") throw new Error("invalid callback: " + t);
    for (; ++o < a; )
      if (r = (e = i[o]).type) n[r] = yi(n[r], e.name, t);
      else if (t == null) for (r in n) n[r] = yi(n[r], e.name, null);
    return this;
  },
  copy: function() {
    var e = {}, t = this._;
    for (var n in t) e[n] = t[n].slice();
    return new fn(e);
  },
  call: function(e, t) {
    if ((r = arguments.length - 2) > 0) for (var n = new Array(r), i = 0, r, o; i < r; ++i) n[i] = arguments[i + 2];
    if (!this._.hasOwnProperty(e)) throw new Error("unknown type: " + e);
    for (o = this._[e], i = 0, r = o.length; i < r; ++i) o[i].value.apply(t, n);
  },
  apply: function(e, t, n) {
    if (!this._.hasOwnProperty(e)) throw new Error("unknown type: " + e);
    for (var i = this._[e], r = 0, o = i.length; r < o; ++r) i[r].value.apply(t, n);
  }
};
function io(e, t) {
  for (var n = 0, i = e.length, r; n < i; ++n)
    if ((r = e[n]).name === t)
      return r.value;
}
function yi(e, t, n) {
  for (var i = 0, r = e.length; i < r; ++i)
    if (e[i].name === t) {
      e[i] = eo, e = e.slice(0, i).concat(e.slice(i + 1));
      break;
    }
  return n != null && e.push({ name: t, value: n }), e;
}
var _e = 0, ze = 0, Le = 0, Hi = 1e3, mn, Be, yn = 0, oe = 0, An = 0, Ge = typeof performance == "object" && performance.now ? performance : Date, Wi = typeof window == "object" && window.requestAnimationFrame ? window.requestAnimationFrame.bind(window) : function(e) {
  setTimeout(e, 17);
};
function Vi() {
  return oe || (Wi(ro), oe = Ge.now() + An);
}
function ro() {
  oe = 0;
}
function Zn() {
  this._call = this._time = this._next = null;
}
Zn.prototype = Yi.prototype = {
  constructor: Zn,
  restart: function(e, t, n) {
    if (typeof e != "function") throw new TypeError("callback is not a function");
    n = (n == null ? Vi() : +n) + (t == null ? 0 : +t), !this._next && Be !== this && (Be ? Be._next = this : mn = this, Be = this), this._call = e, this._time = n, Qn();
  },
  stop: function() {
    this._call && (this._call = null, this._time = 1 / 0, Qn());
  }
};
function Yi(e, t, n) {
  var i = new Zn();
  return i.restart(e, t, n), i;
}
function oo() {
  Vi(), ++_e;
  for (var e = mn, t; e; )
    (t = oe - e._time) >= 0 && e._call.call(void 0, t), e = e._next;
  --_e;
}
function _i() {
  oe = (yn = Ge.now()) + An, _e = ze = 0;
  try {
    oo();
  } finally {
    _e = 0, ao(), oe = 0;
  }
}
function so() {
  var e = Ge.now(), t = e - yn;
  t > Hi && (An -= t, yn = e);
}
function ao() {
  for (var e, t = mn, n, i = 1 / 0; t; )
    t._call ? (i > t._time && (i = t._time), e = t, t = t._next) : (n = t._next, t._next = null, t = e ? e._next = n : mn = n);
  Be = e, Qn(i);
}
function Qn(e) {
  if (!_e) {
    ze && (ze = clearTimeout(ze));
    var t = e - oe;
    t > 24 ? (e < 1 / 0 && (ze = setTimeout(_i, e - Ge.now() - An)), Le && (Le = clearInterval(Le))) : (Le || (yn = Ge.now(), Le = setInterval(so, Hi)), _e = 1, Wi(_i));
  }
}
const lo = 1664525, co = 1013904223, vi = 4294967296;
function uo() {
  let e = 1;
  return () => (e = (lo * e + co) % vi) / vi;
}
function ho(e) {
  return e.x;
}
function fo(e) {
  return e.y;
}
var po = 10, go = Math.PI * (3 - Math.sqrt(5));
function mo(e) {
  var t, n = 1, i = 1e-3, r = 1 - Math.pow(i, 1 / 300), o = 0, a = 0.6, l = /* @__PURE__ */ new Map(), h = Yi(w), s = oi("tick", "end"), f = uo();
  e == null && (e = []);
  function w() {
    m(), s.call("tick", t), n < i && (h.stop(), s.call("end", t));
  }
  function m(y) {
    var _, v = e.length, S;
    y === void 0 && (y = 1);
    for (var x = 0; x < y; ++x)
      for (n += (o - n) * r, l.forEach(function(g) {
        g(n);
      }), _ = 0; _ < v; ++_)
        S = e[_], S.fx == null ? S.x += S.vx *= a : (S.x = S.fx, S.vx = 0), S.fy == null ? S.y += S.vy *= a : (S.y = S.fy, S.vy = 0);
    return t;
  }
  function p() {
    for (var y = 0, _ = e.length, v; y < _; ++y) {
      if (v = e[y], v.index = y, v.fx != null && (v.x = v.fx), v.fy != null && (v.y = v.fy), isNaN(v.x) || isNaN(v.y)) {
        var S = po * Math.sqrt(0.5 + y), x = y * go;
        v.x = S * Math.cos(x), v.y = S * Math.sin(x);
      }
      (isNaN(v.vx) || isNaN(v.vy)) && (v.vx = v.vy = 0);
    }
  }
  function A(y) {
    return y.initialize && y.initialize(e, f), y;
  }
  return p(), t = {
    tick: m,
    restart: function() {
      return h.restart(w), t;
    },
    stop: function() {
      return h.stop(), t;
    },
    nodes: function(y) {
      return arguments.length ? (e = y, p(), l.forEach(A), t) : e;
    },
    alpha: function(y) {
      return arguments.length ? (n = +y, t) : n;
    },
    alphaMin: function(y) {
      return arguments.length ? (i = +y, t) : i;
    },
    alphaDecay: function(y) {
      return arguments.length ? (r = +y, t) : +r;
    },
    alphaTarget: function(y) {
      return arguments.length ? (o = +y, t) : o;
    },
    velocityDecay: function(y) {
      return arguments.length ? (a = 1 - y, t) : 1 - a;
    },
    randomSource: function(y) {
      return arguments.length ? (f = y, l.forEach(A), t) : f;
    },
    force: function(y, _) {
      return arguments.length > 1 ? (_ == null ? l.delete(y) : l.set(y, A(_)), t) : l.get(y);
    },
    find: function(y, _, v) {
      var S = 0, x = e.length, g, I, C, L, F;
      for (v == null ? v = 1 / 0 : v *= v, S = 0; S < x; ++S)
        L = e[S], g = y - L.x, I = _ - L.y, C = g * g + I * I, C < v && (F = L, v = C);
      return F;
    },
    on: function(y, _) {
      return arguments.length > 1 ? (s.on(y, _), t) : s.on(y);
    }
  };
}
function yo() {
  var e, t, n, i, r = nt(-30), o, a = 1, l = 1 / 0, h = 0.81;
  function s(p) {
    var A, y = e.length, _ = ii(e, ho, fo).visitAfter(w);
    for (i = p, A = 0; A < y; ++A) t = e[A], _.visit(m);
  }
  function f() {
    if (e) {
      var p, A = e.length, y;
      for (o = new Array(A), p = 0; p < A; ++p) y = e[p], o[y.index] = +r(y, p, e);
    }
  }
  function w(p) {
    var A = 0, y, _, v = 0, S, x, g;
    if (p.length) {
      for (S = x = g = 0; g < 4; ++g)
        (y = p[g]) && (_ = Math.abs(y.value)) && (A += y.value, v += _, S += _ * y.x, x += _ * y.y);
      p.x = S / v, p.y = x / v;
    } else {
      y = p, y.x = y.data.x, y.y = y.data.y;
      do
        A += o[y.data.index];
      while (y = y.next);
    }
    p.value = A;
  }
  function m(p, A, y, _) {
    if (!p.value) return !0;
    var v = p.x - t.x, S = p.y - t.y, x = _ - A, g = v * v + S * S;
    if (x * x / h < g)
      return g < l && (v === 0 && (v = Zt(n), g += v * v), S === 0 && (S = Zt(n), g += S * S), g < a && (g = Math.sqrt(a * g)), t.vx += v * p.value * i / g, t.vy += S * p.value * i / g), !0;
    if (p.length || g >= l) return;
    (p.data !== t || p.next) && (v === 0 && (v = Zt(n), g += v * v), S === 0 && (S = Zt(n), g += S * S), g < a && (g = Math.sqrt(a * g)));
    do
      p.data !== t && (x = o[p.data.index] * i / g, t.vx += v * x, t.vy += S * x);
    while (p = p.next);
  }
  return s.initialize = function(p, A) {
    e = p, n = A, f();
  }, s.strength = function(p) {
    return arguments.length ? (r = typeof p == "function" ? p : nt(+p), f(), s) : r;
  }, s.distanceMin = function(p) {
    return arguments.length ? (a = p * p, s) : Math.sqrt(a);
  }, s.distanceMax = function(p) {
    return arguments.length ? (l = p * p, s) : Math.sqrt(l);
  }, s.theta = function(p) {
    return arguments.length ? (h = p * p, s) : Math.sqrt(h);
  }, s;
}
function bi(e, t, n) {
  var i, r = nt(0.1), o, a;
  typeof e != "function" && (e = nt(+e)), t == null && (t = 0), n == null && (n = 0);
  function l(s) {
    for (var f = 0, w = i.length; f < w; ++f) {
      var m = i[f], p = m.x - t || 1e-6, A = m.y - n || 1e-6, y = Math.sqrt(p * p + A * A), _ = (a[f] - y) * o[f] * s / y;
      m.vx += p * _, m.vy += A * _;
    }
  }
  function h() {
    if (i) {
      var s, f = i.length;
      for (o = new Array(f), a = new Array(f), s = 0; s < f; ++s)
        a[s] = +e(i[s], s, i), o[s] = isNaN(a[s]) ? 0 : +r(i[s], s, i);
    }
  }
  return l.initialize = function(s) {
    i = s, h();
  }, l.strength = function(s) {
    return arguments.length ? (r = typeof s == "function" ? s : nt(+s), h(), l) : r;
  }, l.radius = function(s) {
    return arguments.length ? (e = typeof s == "function" ? s : nt(+s), h(), l) : e;
  }, l.x = function(s) {
    return arguments.length ? (t = +s, l) : t;
  }, l.y = function(s) {
    return arguments.length ? (n = +s, l) : n;
  }, l;
}
function wi(e) {
  var t = nt(0.1), n, i, r;
  typeof e != "function" && (e = nt(e == null ? 0 : +e));
  function o(l) {
    for (var h = 0, s = n.length, f; h < s; ++h)
      f = n[h], f.vx += (r[h] - f.x) * i[h] * l;
  }
  function a() {
    if (n) {
      var l, h = n.length;
      for (i = new Array(h), r = new Array(h), l = 0; l < h; ++l)
        i[l] = isNaN(r[l] = +e(n[l], l, n)) ? 0 : +t(n[l], l, n);
    }
  }
  return o.initialize = function(l) {
    n = l, a();
  }, o.strength = function(l) {
    return arguments.length ? (t = typeof l == "function" ? l : nt(+l), a(), o) : t;
  }, o.x = function(l) {
    return arguments.length ? (e = typeof l == "function" ? l : nt(+l), a(), o) : e;
  }, o;
}
function Ti(e) {
  var t = nt(0.1), n, i, r;
  typeof e != "function" && (e = nt(e == null ? 0 : +e));
  function o(l) {
    for (var h = 0, s = n.length, f; h < s; ++h)
      f = n[h], f.vy += (r[h] - f.y) * i[h] * l;
  }
  function a() {
    if (n) {
      var l, h = n.length;
      for (i = new Array(h), r = new Array(h), l = 0; l < h; ++l)
        i[l] = isNaN(r[l] = +e(n[l], l, n)) ? 0 : +t(n[l], l, n);
    }
  }
  return o.initialize = function(l) {
    n = l, a();
  }, o.strength = function(l) {
    return arguments.length ? (t = typeof l == "function" ? l : nt(+l), a(), o) : t;
  }, o.y = function(l) {
    return arguments.length ? (e = typeof l == "function" ? l : nt(+l), a(), o) : e;
  }, o;
}
function _o(e = 0, t = 0, n = 1e-3) {
  let i = [], r;
  function o() {
    r = typeof n == "function" ? n : () => n;
  }
  function a(l) {
    for (let h = 0, s = i.length; h < s; ++h) {
      const f = i[h], w = r(f, h, i);
      f.vx && f.x && (f.vx -= (f.x - e) * w * l), f.vy && f.y && (f.vy -= (f.y - t) * w * l);
    }
  }
  return a.initialize = (l) => {
    i = l, o();
  }, a.x = function(l) {
    return arguments.length ? (e = l, a) : e;
  }, a.y = function(l) {
    return arguments.length ? (t = l, a) : t;
  }, a.strength = function(l) {
    return arguments.length ? (n = l, o(), a) : n;
  }, a;
}
var Jn = "http://www.w3.org/1999/xhtml";
const Si = {
  svg: "http://www.w3.org/2000/svg",
  xhtml: Jn,
  xlink: "http://www.w3.org/1999/xlink",
  xml: "http://www.w3.org/XML/1998/namespace",
  xmlns: "http://www.w3.org/2000/xmlns/"
};
function $i(e) {
  var t = e += "", n = t.indexOf(":");
  return n >= 0 && (t = e.slice(0, n)) !== "xmlns" && (e = e.slice(n + 1)), Si.hasOwnProperty(t) ? { space: Si[t], local: e } : e;
}
function vo(e) {
  return function() {
    var t = this.ownerDocument, n = this.namespaceURI;
    return n === Jn && t.documentElement.namespaceURI === Jn ? t.createElement(e) : t.createElementNS(n, e);
  };
}
function bo(e) {
  return function() {
    return this.ownerDocument.createElementNS(e.space, e.local);
  };
}
function qi(e) {
  var t = $i(e);
  return (t.local ? bo : vo)(t);
}
function wo() {
}
function Ki(e) {
  return e == null ? wo : function() {
    return this.querySelector(e);
  };
}
function To(e) {
  typeof e != "function" && (e = Ki(e));
  for (var t = this._groups, n = t.length, i = new Array(n), r = 0; r < n; ++r)
    for (var o = t[r], a = o.length, l = i[r] = new Array(a), h, s, f = 0; f < a; ++f)
      (h = o[f]) && (s = e.call(h, h.__data__, f, o)) && ("__data__" in h && (s.__data__ = h.__data__), l[f] = s);
  return new wt(i, this._parents);
}
function So(e) {
  return e == null ? [] : Array.isArray(e) ? e : Array.from(e);
}
function Ao() {
  return [];
}
function xo(e) {
  return e == null ? Ao : function() {
    return this.querySelectorAll(e);
  };
}
function Eo(e) {
  return function() {
    return So(e.apply(this, arguments));
  };
}
function No(e) {
  typeof e == "function" ? e = Eo(e) : e = xo(e);
  for (var t = this._groups, n = t.length, i = [], r = [], o = 0; o < n; ++o)
    for (var a = t[o], l = a.length, h, s = 0; s < l; ++s)
      (h = a[s]) && (i.push(e.call(h, h.__data__, s, a)), r.push(h));
  return new wt(i, r);
}
function Do(e) {
  return function() {
    return this.matches(e);
  };
}
function Xi(e) {
  return function(t) {
    return t.matches(e);
  };
}
var Ro = Array.prototype.find;
function Io(e) {
  return function() {
    return Ro.call(this.children, e);
  };
}
function Co() {
  return this.firstElementChild;
}
function Oo(e) {
  return this.select(e == null ? Co : Io(typeof e == "function" ? e : Xi(e)));
}
var Mo = Array.prototype.filter;
function ko() {
  return Array.from(this.children);
}
function Lo(e) {
  return function() {
    return Mo.call(this.children, e);
  };
}
function Fo(e) {
  return this.selectAll(e == null ? ko : Lo(typeof e == "function" ? e : Xi(e)));
}
function Po(e) {
  typeof e != "function" && (e = Do(e));
  for (var t = this._groups, n = t.length, i = new Array(n), r = 0; r < n; ++r)
    for (var o = t[r], a = o.length, l = i[r] = [], h, s = 0; s < a; ++s)
      (h = o[s]) && e.call(h, h.__data__, s, o) && l.push(h);
  return new wt(i, this._parents);
}
function Zi(e) {
  return new Array(e.length);
}
function zo() {
  return new wt(this._enter || this._groups.map(Zi), this._parents);
}
function _n(e, t) {
  this.ownerDocument = e.ownerDocument, this.namespaceURI = e.namespaceURI, this._next = null, this._parent = e, this.__data__ = t;
}
_n.prototype = {
  constructor: _n,
  appendChild: function(e) {
    return this._parent.insertBefore(e, this._next);
  },
  insertBefore: function(e, t) {
    return this._parent.insertBefore(e, t);
  },
  querySelector: function(e) {
    return this._parent.querySelector(e);
  },
  querySelectorAll: function(e) {
    return this._parent.querySelectorAll(e);
  }
};
function Bo(e) {
  return function() {
    return e;
  };
}
function Uo(e, t, n, i, r, o) {
  for (var a = 0, l, h = t.length, s = o.length; a < s; ++a)
    (l = t[a]) ? (l.__data__ = o[a], i[a] = l) : n[a] = new _n(e, o[a]);
  for (; a < h; ++a)
    (l = t[a]) && (r[a] = l);
}
function jo(e, t, n, i, r, o, a) {
  var l, h, s = /* @__PURE__ */ new Map(), f = t.length, w = o.length, m = new Array(f), p;
  for (l = 0; l < f; ++l)
    (h = t[l]) && (m[l] = p = a.call(h, h.__data__, l, t) + "", s.has(p) ? r[l] = h : s.set(p, h));
  for (l = 0; l < w; ++l)
    p = a.call(e, o[l], l, o) + "", (h = s.get(p)) ? (i[l] = h, h.__data__ = o[l], s.delete(p)) : n[l] = new _n(e, o[l]);
  for (l = 0; l < f; ++l)
    (h = t[l]) && s.get(m[l]) === h && (r[l] = h);
}
function Go(e) {
  return e.__data__;
}
function Ho(e, t) {
  if (!arguments.length) return Array.from(this, Go);
  var n = t ? jo : Uo, i = this._parents, r = this._groups;
  typeof e != "function" && (e = Bo(e));
  for (var o = r.length, a = new Array(o), l = new Array(o), h = new Array(o), s = 0; s < o; ++s) {
    var f = i[s], w = r[s], m = w.length, p = Wo(e.call(f, f && f.__data__, s, i)), A = p.length, y = l[s] = new Array(A), _ = a[s] = new Array(A), v = h[s] = new Array(m);
    n(f, w, y, _, v, p, t);
    for (var S = 0, x = 0, g, I; S < A; ++S)
      if (g = y[S]) {
        for (S >= x && (x = S + 1); !(I = _[x]) && ++x < A; ) ;
        g._next = I || null;
      }
  }
  return a = new wt(a, i), a._enter = l, a._exit = h, a;
}
function Wo(e) {
  return typeof e == "object" && "length" in e ? e : Array.from(e);
}
function Vo() {
  return new wt(this._exit || this._groups.map(Zi), this._parents);
}
function Yo(e, t, n) {
  var i = this.enter(), r = this, o = this.exit();
  return typeof e == "function" ? (i = e(i), i && (i = i.selection())) : i = i.append(e + ""), t != null && (r = t(r), r && (r = r.selection())), n == null ? o.remove() : n(o), i && r ? i.merge(r).order() : r;
}
function $o(e) {
  for (var t = e.selection ? e.selection() : e, n = this._groups, i = t._groups, r = n.length, o = i.length, a = Math.min(r, o), l = new Array(r), h = 0; h < a; ++h)
    for (var s = n[h], f = i[h], w = s.length, m = l[h] = new Array(w), p, A = 0; A < w; ++A)
      (p = s[A] || f[A]) && (m[A] = p);
  for (; h < r; ++h)
    l[h] = n[h];
  return new wt(l, this._parents);
}
function qo() {
  for (var e = this._groups, t = -1, n = e.length; ++t < n; )
    for (var i = e[t], r = i.length - 1, o = i[r], a; --r >= 0; )
      (a = i[r]) && (o && a.compareDocumentPosition(o) ^ 4 && o.parentNode.insertBefore(a, o), o = a);
  return this;
}
function Ko(e) {
  e || (e = Xo);
  function t(w, m) {
    return w && m ? e(w.__data__, m.__data__) : !w - !m;
  }
  for (var n = this._groups, i = n.length, r = new Array(i), o = 0; o < i; ++o) {
    for (var a = n[o], l = a.length, h = r[o] = new Array(l), s, f = 0; f < l; ++f)
      (s = a[f]) && (h[f] = s);
    h.sort(t);
  }
  return new wt(r, this._parents).order();
}
function Xo(e, t) {
  return e < t ? -1 : e > t ? 1 : e >= t ? 0 : NaN;
}
function Zo() {
  var e = arguments[0];
  return arguments[0] = this, e.apply(null, arguments), this;
}
function Qo() {
  return Array.from(this);
}
function Jo() {
  for (var e = this._groups, t = 0, n = e.length; t < n; ++t)
    for (var i = e[t], r = 0, o = i.length; r < o; ++r) {
      var a = i[r];
      if (a) return a;
    }
  return null;
}
function ts() {
  let e = 0;
  for (const t of this) ++e;
  return e;
}
function es() {
  return !this.node();
}
function ns(e) {
  for (var t = this._groups, n = 0, i = t.length; n < i; ++n)
    for (var r = t[n], o = 0, a = r.length, l; o < a; ++o)
      (l = r[o]) && e.call(l, l.__data__, o, r);
  return this;
}
function is(e) {
  return function() {
    this.removeAttribute(e);
  };
}
function rs(e) {
  return function() {
    this.removeAttributeNS(e.space, e.local);
  };
}
function os(e, t) {
  return function() {
    this.setAttribute(e, t);
  };
}
function ss(e, t) {
  return function() {
    this.setAttributeNS(e.space, e.local, t);
  };
}
function as(e, t) {
  return function() {
    var n = t.apply(this, arguments);
    n == null ? this.removeAttribute(e) : this.setAttribute(e, n);
  };
}
function ls(e, t) {
  return function() {
    var n = t.apply(this, arguments);
    n == null ? this.removeAttributeNS(e.space, e.local) : this.setAttributeNS(e.space, e.local, n);
  };
}
function cs(e, t) {
  var n = $i(e);
  if (arguments.length < 2) {
    var i = this.node();
    return n.local ? i.getAttributeNS(n.space, n.local) : i.getAttribute(n);
  }
  return this.each((t == null ? n.local ? rs : is : typeof t == "function" ? n.local ? ls : as : n.local ? ss : os)(n, t));
}
function Qi(e) {
  return e.ownerDocument && e.ownerDocument.defaultView || e.document && e || e.defaultView;
}
function us(e) {
  return function() {
    this.style.removeProperty(e);
  };
}
function hs(e, t, n) {
  return function() {
    this.style.setProperty(e, t, n);
  };
}
function fs(e, t, n) {
  return function() {
    var i = t.apply(this, arguments);
    i == null ? this.style.removeProperty(e) : this.style.setProperty(e, i, n);
  };
}
function ds(e, t, n) {
  return arguments.length > 1 ? this.each((t == null ? us : typeof t == "function" ? fs : hs)(e, t, n ?? "")) : ps(this.node(), e);
}
function ps(e, t) {
  return e.style.getPropertyValue(t) || Qi(e).getComputedStyle(e, null).getPropertyValue(t);
}
function gs(e) {
  return function() {
    delete this[e];
  };
}
function ms(e, t) {
  return function() {
    this[e] = t;
  };
}
function ys(e, t) {
  return function() {
    var n = t.apply(this, arguments);
    n == null ? delete this[e] : this[e] = n;
  };
}
function _s(e, t) {
  return arguments.length > 1 ? this.each((t == null ? gs : typeof t == "function" ? ys : ms)(e, t)) : this.node()[e];
}
function Ji(e) {
  return e.trim().split(/^|\s+/);
}
function si(e) {
  return e.classList || new tr(e);
}
function tr(e) {
  this._node = e, this._names = Ji(e.getAttribute("class") || "");
}
tr.prototype = {
  add: function(e) {
    var t = this._names.indexOf(e);
    t < 0 && (this._names.push(e), this._node.setAttribute("class", this._names.join(" ")));
  },
  remove: function(e) {
    var t = this._names.indexOf(e);
    t >= 0 && (this._names.splice(t, 1), this._node.setAttribute("class", this._names.join(" ")));
  },
  contains: function(e) {
    return this._names.indexOf(e) >= 0;
  }
};
function er(e, t) {
  for (var n = si(e), i = -1, r = t.length; ++i < r; ) n.add(t[i]);
}
function nr(e, t) {
  for (var n = si(e), i = -1, r = t.length; ++i < r; ) n.remove(t[i]);
}
function vs(e) {
  return function() {
    er(this, e);
  };
}
function bs(e) {
  return function() {
    nr(this, e);
  };
}
function ws(e, t) {
  return function() {
    (t.apply(this, arguments) ? er : nr)(this, e);
  };
}
function Ts(e, t) {
  var n = Ji(e + "");
  if (arguments.length < 2) {
    for (var i = si(this.node()), r = -1, o = n.length; ++r < o; ) if (!i.contains(n[r])) return !1;
    return !0;
  }
  return this.each((typeof t == "function" ? ws : t ? vs : bs)(n, t));
}
function Ss() {
  this.textContent = "";
}
function As(e) {
  return function() {
    this.textContent = e;
  };
}
function xs(e) {
  return function() {
    var t = e.apply(this, arguments);
    this.textContent = t ?? "";
  };
}
function Es(e) {
  return arguments.length ? this.each(e == null ? Ss : (typeof e == "function" ? xs : As)(e)) : this.node().textContent;
}
function Ns() {
  this.innerHTML = "";
}
function Ds(e) {
  return function() {
    this.innerHTML = e;
  };
}
function Rs(e) {
  return function() {
    var t = e.apply(this, arguments);
    this.innerHTML = t ?? "";
  };
}
function Is(e) {
  return arguments.length ? this.each(e == null ? Ns : (typeof e == "function" ? Rs : Ds)(e)) : this.node().innerHTML;
}
function Cs() {
  this.nextSibling && this.parentNode.appendChild(this);
}
function Os() {
  return this.each(Cs);
}
function Ms() {
  this.previousSibling && this.parentNode.insertBefore(this, this.parentNode.firstChild);
}
function ks() {
  return this.each(Ms);
}
function Ls(e) {
  var t = typeof e == "function" ? e : qi(e);
  return this.select(function() {
    return this.appendChild(t.apply(this, arguments));
  });
}
function Fs() {
  return null;
}
function Ps(e, t) {
  var n = typeof e == "function" ? e : qi(e), i = t == null ? Fs : typeof t == "function" ? t : Ki(t);
  return this.select(function() {
    return this.insertBefore(n.apply(this, arguments), i.apply(this, arguments) || null);
  });
}
function zs() {
  var e = this.parentNode;
  e && e.removeChild(this);
}
function Bs() {
  return this.each(zs);
}
function Us() {
  var e = this.cloneNode(!1), t = this.parentNode;
  return t ? t.insertBefore(e, this.nextSibling) : e;
}
function js() {
  var e = this.cloneNode(!0), t = this.parentNode;
  return t ? t.insertBefore(e, this.nextSibling) : e;
}
function Gs(e) {
  return this.select(e ? js : Us);
}
function Hs(e) {
  return arguments.length ? this.property("__data__", e) : this.node().__data__;
}
function Ws(e) {
  return function(t) {
    e.call(this, t, this.__data__);
  };
}
function Vs(e) {
  return e.trim().split(/^|\s+/).map(function(t) {
    var n = "", i = t.indexOf(".");
    return i >= 0 && (n = t.slice(i + 1), t = t.slice(0, i)), { type: t, name: n };
  });
}
function Ys(e) {
  return function() {
    var t = this.__on;
    if (t) {
      for (var n = 0, i = -1, r = t.length, o; n < r; ++n)
        o = t[n], (!e.type || o.type === e.type) && o.name === e.name ? this.removeEventListener(o.type, o.listener, o.options) : t[++i] = o;
      ++i ? t.length = i : delete this.__on;
    }
  };
}
function $s(e, t, n) {
  return function() {
    var i = this.__on, r, o = Ws(t);
    if (i) {
      for (var a = 0, l = i.length; a < l; ++a)
        if ((r = i[a]).type === e.type && r.name === e.name) {
          this.removeEventListener(r.type, r.listener, r.options), this.addEventListener(r.type, r.listener = o, r.options = n), r.value = t;
          return;
        }
    }
    this.addEventListener(e.type, o, n), r = { type: e.type, name: e.name, value: t, listener: o, options: n }, i ? i.push(r) : this.__on = [r];
  };
}
function qs(e, t, n) {
  var i = Vs(e + ""), r, o = i.length, a;
  if (arguments.length < 2) {
    var l = this.node().__on;
    if (l) {
      for (var h = 0, s = l.length, f; h < s; ++h)
        for (r = 0, f = l[h]; r < o; ++r)
          if ((a = i[r]).type === f.type && a.name === f.name)
            return f.value;
    }
    return;
  }
  for (l = t ? $s : Ys, r = 0; r < o; ++r) this.each(l(i[r], t, n));
  return this;
}
function ir(e, t, n) {
  var i = Qi(e), r = i.CustomEvent;
  typeof r == "function" ? r = new r(t, n) : (r = i.document.createEvent("Event"), n ? (r.initEvent(t, n.bubbles, n.cancelable), r.detail = n.detail) : r.initEvent(t, !1, !1)), e.dispatchEvent(r);
}
function Ks(e, t) {
  return function() {
    return ir(this, e, t);
  };
}
function Xs(e, t) {
  return function() {
    return ir(this, e, t.apply(this, arguments));
  };
}
function Zs(e, t) {
  return this.each((typeof t == "function" ? Xs : Ks)(e, t));
}
function* Qs() {
  for (var e = this._groups, t = 0, n = e.length; t < n; ++t)
    for (var i = e[t], r = 0, o = i.length, a; r < o; ++r)
      (a = i[r]) && (yield a);
}
var Js = [null];
function wt(e, t) {
  this._groups = e, this._parents = t;
}
function ta() {
  return this;
}
wt.prototype = {
  constructor: wt,
  select: To,
  selectAll: No,
  selectChild: Oo,
  selectChildren: Fo,
  filter: Po,
  data: Ho,
  enter: zo,
  exit: Vo,
  join: Yo,
  merge: $o,
  selection: ta,
  order: qo,
  sort: Ko,
  call: Zo,
  nodes: Qo,
  node: Jo,
  size: ts,
  empty: es,
  each: ns,
  attr: cs,
  style: ds,
  property: _s,
  classed: Ts,
  text: Es,
  html: Is,
  raise: Os,
  lower: ks,
  append: Ls,
  insert: Ps,
  remove: Bs,
  clone: Gs,
  datum: Hs,
  on: qs,
  dispatch: Zs,
  [Symbol.iterator]: Qs
};
function vn(e) {
  return typeof e == "string" ? new wt([[document.querySelector(e)]], [document.documentElement]) : new wt([[e]], Js);
}
function ea(e) {
  let t;
  for (; t = e.sourceEvent; ) e = t;
  return e;
}
function Ai(e, t) {
  if (e = ea(e), t === void 0 && (t = e.currentTarget), t) {
    var n = t.ownerSVGElement || t;
    if (n.createSVGPoint) {
      var i = n.createSVGPoint();
      return i.x = e.clientX, i.y = e.clientY, i = i.matrixTransform(t.getScreenCTM().inverse()), [i.x, i.y];
    }
    if (t.getBoundingClientRect) {
      var r = t.getBoundingClientRect();
      return [e.clientX - r.left - t.clientLeft, e.clientY - r.top - t.clientTop];
    }
  }
  return [e.pageX, e.pageY];
}
const na = { passive: !1 }, He = { capture: !0, passive: !1 };
function Gn(e) {
  e.stopImmediatePropagation();
}
function ye(e) {
  e.preventDefault(), e.stopImmediatePropagation();
}
function ia(e) {
  var t = e.document.documentElement, n = vn(e).on("dragstart.drag", ye, He);
  "onselectstart" in t ? n.on("selectstart.drag", ye, He) : (t.__noselect = t.style.MozUserSelect, t.style.MozUserSelect = "none");
}
function ra(e, t) {
  var n = e.document.documentElement, i = vn(e).on("dragstart.drag", null);
  t && (i.on("click.drag", ye, He), setTimeout(function() {
    i.on("click.drag", null);
  }, 0)), "onselectstart" in n ? i.on("selectstart.drag", null) : (n.style.MozUserSelect = n.__noselect, delete n.__noselect);
}
const cn = (e) => () => e;
function ti(e, {
  sourceEvent: t,
  subject: n,
  target: i,
  identifier: r,
  active: o,
  x: a,
  y: l,
  dx: h,
  dy: s,
  dispatch: f
}) {
  Object.defineProperties(this, {
    type: { value: e, enumerable: !0, configurable: !0 },
    sourceEvent: { value: t, enumerable: !0, configurable: !0 },
    subject: { value: n, enumerable: !0, configurable: !0 },
    target: { value: i, enumerable: !0, configurable: !0 },
    identifier: { value: r, enumerable: !0, configurable: !0 },
    active: { value: o, enumerable: !0, configurable: !0 },
    x: { value: a, enumerable: !0, configurable: !0 },
    y: { value: l, enumerable: !0, configurable: !0 },
    dx: { value: h, enumerable: !0, configurable: !0 },
    dy: { value: s, enumerable: !0, configurable: !0 },
    _: { value: f }
  });
}
ti.prototype.on = function() {
  var e = this._.on.apply(this._, arguments);
  return e === this._ ? this : e;
};
function oa(e) {
  return !e.ctrlKey && !e.button;
}
function sa() {
  return this.parentNode;
}
function aa(e, t) {
  return t ?? { x: e.x, y: e.y };
}
function la() {
  return navigator.maxTouchPoints || "ontouchstart" in this;
}
function ca() {
  var e = oa, t = sa, n = aa, i = la, r = {}, o = oi("start", "drag", "end"), a = 0, l, h, s, f, w = 0;
  function m(g) {
    g.on("mousedown.drag", p).filter(i).on("touchstart.drag", _).on("touchmove.drag", v, na).on("touchend.drag touchcancel.drag", S).style("touch-action", "none").style("-webkit-tap-highlight-color", "rgba(0,0,0,0)");
  }
  function p(g, I) {
    if (!(f || !e.call(this, g, I))) {
      var C = x(this, t.call(this, g, I), g, I, "mouse");
      C && (vn(g.view).on("mousemove.drag", A, He).on("mouseup.drag", y, He), ia(g.view), Gn(g), s = !1, l = g.clientX, h = g.clientY, C("start", g));
    }
  }
  function A(g) {
    if (ye(g), !s) {
      var I = g.clientX - l, C = g.clientY - h;
      s = I * I + C * C > w;
    }
    r.mouse("drag", g);
  }
  function y(g) {
    vn(g.view).on("mousemove.drag mouseup.drag", null), ra(g.view, s), ye(g), r.mouse("end", g);
  }
  function _(g, I) {
    if (e.call(this, g, I)) {
      var C = g.changedTouches, L = t.call(this, g, I), F = C.length, Y, j;
      for (Y = 0; Y < F; ++Y)
        (j = x(this, L, g, I, C[Y].identifier, C[Y])) && (Gn(g), j("start", g, C[Y]));
    }
  }
  function v(g) {
    var I = g.changedTouches, C = I.length, L, F;
    for (L = 0; L < C; ++L)
      (F = r[I[L].identifier]) && (ye(g), F("drag", g, I[L]));
  }
  function S(g) {
    var I = g.changedTouches, C = I.length, L, F;
    for (f && clearTimeout(f), f = setTimeout(function() {
      f = null;
    }, 500), L = 0; L < C; ++L)
      (F = r[I[L].identifier]) && (Gn(g), F("end", g, I[L]));
  }
  function x(g, I, C, L, F, Y) {
    var j = o.copy(), Q = Ai(Y || C, I), at, ot, q;
    if ((q = n.call(g, new ti("beforestart", {
      sourceEvent: C,
      target: m,
      identifier: F,
      active: a,
      x: Q[0],
      y: Q[1],
      dx: 0,
      dy: 0,
      dispatch: j
    }), L)) != null)
      return at = q.x - Q[0] || 0, ot = q.y - Q[1] || 0, function Ht(Nt, kt, ve) {
        var se = Q, Qt;
        switch (Nt) {
          case "start":
            r[F] = Ht, Qt = a++;
            break;
          case "end":
            delete r[F], --a;
          // falls through
          case "drag":
            Q = Ai(ve || kt, I), Qt = a;
            break;
        }
        j.call(
          Nt,
          g,
          new ti(Nt, {
            sourceEvent: kt,
            subject: q,
            target: m,
            identifier: F,
            active: Qt,
            x: Q[0] + at,
            y: Q[1] + ot,
            dx: Q[0] - se[0],
            dy: Q[1] - se[1],
            dispatch: j
          }),
          L
        );
      };
  }
  return m.filter = function(g) {
    return arguments.length ? (e = typeof g == "function" ? g : cn(!!g), m) : e;
  }, m.container = function(g) {
    return arguments.length ? (t = typeof g == "function" ? g : cn(g), m) : t;
  }, m.subject = function(g) {
    return arguments.length ? (n = typeof g == "function" ? g : cn(g), m) : n;
  }, m.touchable = function(g) {
    return arguments.length ? (i = typeof g == "function" ? g : cn(!!g), m) : i;
  }, m.on = function() {
    var g = o.on.apply(o, arguments);
    return g === o ? m : g;
  }, m.clickDistance = function(g) {
    return arguments.length ? (w = (g = +g) * g, m) : Math.sqrt(w);
  }, m;
}
/*! @license DOMPurify 3.4.7 | (c) Cure53 and other contributors | Released under the Apache license 2.0 and Mozilla Public License 2.0 | github.com/cure53/DOMPurify/blob/3.4.7/LICENSE */
function xi(e, t) {
  (t == null || t > e.length) && (t = e.length);
  for (var n = 0, i = Array(t); n < t; n++) i[n] = e[n];
  return i;
}
function ua(e) {
  if (Array.isArray(e)) return e;
}
function ha(e, t) {
  var n = e == null ? null : typeof Symbol < "u" && e[Symbol.iterator] || e["@@iterator"];
  if (n != null) {
    var i, r, o, a, l = [], h = !0, s = !1;
    try {
      if (o = (n = n.call(e)).next, t !== 0) for (; !(h = (i = o.call(n)).done) && (l.push(i.value), l.length !== t); h = !0) ;
    } catch (f) {
      s = !0, r = f;
    } finally {
      try {
        if (!h && n.return != null && (a = n.return(), Object(a) !== a)) return;
      } finally {
        if (s) throw r;
      }
    }
    return l;
  }
}
function fa() {
  throw new TypeError(`Invalid attempt to destructure non-iterable instance.
In order to be iterable, non-array objects must have a [Symbol.iterator]() method.`);
}
function da(e, t) {
  return ua(e) || ha(e, t) || pa(e, t) || fa();
}
function pa(e, t) {
  if (e) {
    if (typeof e == "string") return xi(e, t);
    var n = {}.toString.call(e).slice(8, -1);
    return n === "Object" && e.constructor && (n = e.constructor.name), n === "Map" || n === "Set" ? Array.from(e) : n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n) ? xi(e, t) : void 0;
  }
}
const rr = Object.entries, Ei = Object.setPrototypeOf, ga = Object.isFrozen, ma = Object.getPrototypeOf, ya = Object.getOwnPropertyDescriptor;
let ut = Object.freeze, Tt = Object.seal, me = Object.create, or = typeof Reflect < "u" && Reflect, ei = or.apply, ni = or.construct;
ut || (ut = function(t) {
  return t;
});
Tt || (Tt = function(t) {
  return t;
});
ei || (ei = function(t, n) {
  for (var i = arguments.length, r = new Array(i > 2 ? i - 2 : 0), o = 2; o < i; o++)
    r[o - 2] = arguments[o];
  return t.apply(n, r);
});
ni || (ni = function(t) {
  for (var n = arguments.length, i = new Array(n > 1 ? n - 1 : 0), r = 1; r < n; r++)
    i[r - 1] = arguments[r];
  return new t(...i);
});
const de = et(Array.prototype.forEach), _a = et(Array.prototype.lastIndexOf), Ni = et(Array.prototype.pop), pe = et(Array.prototype.push), va = et(Array.prototype.splice), lt = Array.isArray, Ue = et(String.prototype.toLowerCase), Hn = et(String.prototype.toString), Di = et(String.prototype.match), ge = et(String.prototype.replace), Ri = et(String.prototype.indexOf), ba = et(String.prototype.trim), wa = et(Number.prototype.toString), Ta = et(Boolean.prototype.toString), Ii = typeof BigInt > "u" ? null : et(BigInt.prototype.toString), Ci = typeof Symbol > "u" ? null : et(Symbol.prototype.toString), J = et(Object.prototype.hasOwnProperty), Fe = et(Object.prototype.toString), rt = et(RegExp.prototype.test), Pe = Sa(TypeError);
function et(e) {
  return function(t) {
    t instanceof RegExp && (t.lastIndex = 0);
    for (var n = arguments.length, i = new Array(n > 1 ? n - 1 : 0), r = 1; r < n; r++)
      i[r - 1] = arguments[r];
    return ei(e, t, i);
  };
}
function Sa(e) {
  return function() {
    for (var t = arguments.length, n = new Array(t), i = 0; i < t; i++)
      n[i] = arguments[i];
    return ni(e, n);
  };
}
function M(e, t) {
  let n = arguments.length > 2 && arguments[2] !== void 0 ? arguments[2] : Ue;
  if (Ei && Ei(e, null), !lt(t))
    return e;
  let i = t.length;
  for (; i--; ) {
    let r = t[i];
    if (typeof r == "string") {
      const o = n(r);
      o !== r && (ga(t) || (t[i] = o), r = o);
    }
    e[r] = !0;
  }
  return e;
}
function Aa(e) {
  for (let t = 0; t < e.length; t++)
    J(e, t) || (e[t] = null);
  return e;
}
function st(e) {
  const t = me(null);
  for (const i of rr(e)) {
    var n = da(i, 2);
    const r = n[0], o = n[1];
    J(e, r) && (lt(o) ? t[r] = Aa(o) : o && typeof o == "object" && o.constructor === Object ? t[r] = st(o) : t[r] = o);
  }
  return t;
}
function xa(e) {
  switch (typeof e) {
    case "string":
      return e;
    case "number":
      return wa(e);
    case "boolean":
      return Ta(e);
    case "bigint":
      return Ii ? Ii(e) : "0";
    case "symbol":
      return Ci ? Ci(e) : "Symbol()";
    case "undefined":
      return Fe(e);
    case "function":
    case "object": {
      if (e === null)
        return Fe(e);
      const t = e, n = Mt(t, "toString");
      if (typeof n == "function") {
        const i = n(t);
        return typeof i == "string" ? i : Fe(i);
      }
      return Fe(e);
    }
    default:
      return Fe(e);
  }
}
function Mt(e, t) {
  for (; e !== null; ) {
    const i = ya(e, t);
    if (i) {
      if (i.get)
        return et(i.get);
      if (typeof i.value == "function")
        return et(i.value);
    }
    e = ma(e);
  }
  function n() {
    return null;
  }
  return n;
}
function Ea(e) {
  try {
    return rt(e, ""), !0;
  } catch {
    return !1;
  }
}
const Oi = ut(["a", "abbr", "acronym", "address", "area", "article", "aside", "audio", "b", "bdi", "bdo", "big", "blink", "blockquote", "body", "br", "button", "canvas", "caption", "center", "cite", "code", "col", "colgroup", "content", "data", "datalist", "dd", "decorator", "del", "details", "dfn", "dialog", "dir", "div", "dl", "dt", "element", "em", "fieldset", "figcaption", "figure", "font", "footer", "form", "h1", "h2", "h3", "h4", "h5", "h6", "head", "header", "hgroup", "hr", "html", "i", "img", "input", "ins", "kbd", "label", "legend", "li", "main", "map", "mark", "marquee", "menu", "menuitem", "meter", "nav", "nobr", "ol", "optgroup", "option", "output", "p", "picture", "pre", "progress", "q", "rp", "rt", "ruby", "s", "samp", "search", "section", "select", "shadow", "slot", "small", "source", "spacer", "span", "strike", "strong", "style", "sub", "summary", "sup", "table", "tbody", "td", "template", "textarea", "tfoot", "th", "thead", "time", "tr", "track", "tt", "u", "ul", "var", "video", "wbr"]), Wn = ut(["svg", "a", "altglyph", "altglyphdef", "altglyphitem", "animatecolor", "animatemotion", "animatetransform", "circle", "clippath", "defs", "desc", "ellipse", "enterkeyhint", "exportparts", "filter", "font", "g", "glyph", "glyphref", "hkern", "image", "inputmode", "line", "lineargradient", "marker", "mask", "metadata", "mpath", "part", "path", "pattern", "polygon", "polyline", "radialgradient", "rect", "stop", "style", "switch", "symbol", "text", "textpath", "title", "tref", "tspan", "view", "vkern"]), Vn = ut(["feBlend", "feColorMatrix", "feComponentTransfer", "feComposite", "feConvolveMatrix", "feDiffuseLighting", "feDisplacementMap", "feDistantLight", "feDropShadow", "feFlood", "feFuncA", "feFuncB", "feFuncG", "feFuncR", "feGaussianBlur", "feImage", "feMerge", "feMergeNode", "feMorphology", "feOffset", "fePointLight", "feSpecularLighting", "feSpotLight", "feTile", "feTurbulence"]), Na = ut(["animate", "color-profile", "cursor", "discard", "font-face", "font-face-format", "font-face-name", "font-face-src", "font-face-uri", "foreignobject", "hatch", "hatchpath", "mesh", "meshgradient", "meshpatch", "meshrow", "missing-glyph", "script", "set", "solidcolor", "unknown", "use"]), Yn = ut(["math", "menclose", "merror", "mfenced", "mfrac", "mglyph", "mi", "mlabeledtr", "mmultiscripts", "mn", "mo", "mover", "mpadded", "mphantom", "mroot", "mrow", "ms", "mspace", "msqrt", "mstyle", "msub", "msup", "msubsup", "mtable", "mtd", "mtext", "mtr", "munder", "munderover", "mprescripts"]), Da = ut(["maction", "maligngroup", "malignmark", "mlongdiv", "mscarries", "mscarry", "msgroup", "mstack", "msline", "msrow", "semantics", "annotation", "annotation-xml", "mprescripts", "none"]), Mi = ut(["#text"]), ki = ut(["accept", "action", "align", "alt", "autocapitalize", "autocomplete", "autopictureinpicture", "autoplay", "background", "bgcolor", "border", "capture", "cellpadding", "cellspacing", "checked", "cite", "class", "clear", "color", "cols", "colspan", "command", "commandfor", "controls", "controlslist", "coords", "crossorigin", "datetime", "decoding", "default", "dir", "disabled", "disablepictureinpicture", "disableremoteplayback", "download", "draggable", "enctype", "enterkeyhint", "exportparts", "face", "for", "headers", "height", "hidden", "high", "href", "hreflang", "id", "inert", "inputmode", "integrity", "ismap", "kind", "label", "lang", "list", "loading", "loop", "low", "max", "maxlength", "media", "method", "min", "minlength", "multiple", "muted", "name", "nonce", "noshade", "novalidate", "nowrap", "open", "optimum", "part", "pattern", "placeholder", "playsinline", "popover", "popovertarget", "popovertargetaction", "poster", "preload", "pubdate", "radiogroup", "readonly", "rel", "required", "rev", "reversed", "role", "rows", "rowspan", "spellcheck", "scope", "selected", "shape", "size", "sizes", "slot", "span", "srclang", "start", "src", "srcset", "step", "style", "summary", "tabindex", "title", "translate", "type", "usemap", "valign", "value", "width", "wrap", "xmlns"]), $n = ut(["accent-height", "accumulate", "additive", "alignment-baseline", "amplitude", "ascent", "attributename", "attributetype", "azimuth", "basefrequency", "baseline-shift", "begin", "bias", "by", "class", "clip", "clippathunits", "clip-path", "clip-rule", "color", "color-interpolation", "color-interpolation-filters", "color-profile", "color-rendering", "cx", "cy", "d", "dx", "dy", "diffuseconstant", "direction", "display", "divisor", "dur", "edgemode", "elevation", "end", "exponent", "fill", "fill-opacity", "fill-rule", "filter", "filterunits", "flood-color", "flood-opacity", "font-family", "font-size", "font-size-adjust", "font-stretch", "font-style", "font-variant", "font-weight", "fx", "fy", "g1", "g2", "glyph-name", "glyphref", "gradientunits", "gradienttransform", "height", "href", "id", "image-rendering", "in", "in2", "intercept", "k", "k1", "k2", "k3", "k4", "kerning", "keypoints", "keysplines", "keytimes", "lang", "lengthadjust", "letter-spacing", "kernelmatrix", "kernelunitlength", "lighting-color", "local", "marker-end", "marker-mid", "marker-start", "markerheight", "markerunits", "markerwidth", "maskcontentunits", "maskunits", "max", "mask", "mask-type", "media", "method", "mode", "min", "name", "numoctaves", "offset", "operator", "opacity", "order", "orient", "orientation", "origin", "overflow", "paint-order", "path", "pathlength", "patterncontentunits", "patterntransform", "patternunits", "points", "preservealpha", "preserveaspectratio", "primitiveunits", "r", "rx", "ry", "radius", "refx", "refy", "repeatcount", "repeatdur", "restart", "result", "rotate", "scale", "seed", "shape-rendering", "slope", "specularconstant", "specularexponent", "spreadmethod", "startoffset", "stddeviation", "stitchtiles", "stop-color", "stop-opacity", "stroke-dasharray", "stroke-dashoffset", "stroke-linecap", "stroke-linejoin", "stroke-miterlimit", "stroke-opacity", "stroke", "stroke-width", "style", "surfacescale", "systemlanguage", "tabindex", "tablevalues", "targetx", "targety", "transform", "transform-origin", "text-anchor", "text-decoration", "text-rendering", "textlength", "type", "u1", "u2", "unicode", "values", "viewbox", "visibility", "version", "vert-adv-y", "vert-origin-x", "vert-origin-y", "width", "word-spacing", "wrap", "writing-mode", "xchannelselector", "ychannelselector", "x", "x1", "x2", "xmlns", "y", "y1", "y2", "z", "zoomandpan"]), Li = ut(["accent", "accentunder", "align", "bevelled", "close", "columnalign", "columnlines", "columnspacing", "columnspan", "denomalign", "depth", "dir", "display", "displaystyle", "encoding", "fence", "frame", "height", "href", "id", "largeop", "length", "linethickness", "lquote", "lspace", "mathbackground", "mathcolor", "mathsize", "mathvariant", "maxsize", "minsize", "movablelimits", "notation", "numalign", "open", "rowalign", "rowlines", "rowspacing", "rowspan", "rspace", "rquote", "scriptlevel", "scriptminsize", "scriptsizemultiplier", "selection", "separator", "separators", "stretchy", "subscriptshift", "supscriptshift", "symmetric", "voffset", "width", "xmlns"]), un = ut(["xlink:href", "xml:id", "xlink:title", "xml:space", "xmlns:xlink"]), Ra = Tt(/{{[\w\W]*|^[\w\W]*}}/g), Ia = Tt(/<%[\w\W]*|^[\w\W]*%>/g), Ca = Tt(/\${[\w\W]*/g), Oa = Tt(/^data-[\-\w.\u00B7-\uFFFF]+$/), Ma = Tt(/^aria-[\-\w]+$/), Fi = Tt(
  /^(?:(?:(?:f|ht)tps?|mailto|tel|callto|sms|cid|xmpp|matrix):|[^a-z]|[a-z+.\-]+(?:[^a-z+.\-:]|$))/i
  // eslint-disable-line no-useless-escape
), ka = Tt(/^(?:\w+script|data):/i), La = Tt(
  /[\u0000-\u0020\u00A0\u1680\u180E\u2000-\u2029\u205F\u3000]/g
  // eslint-disable-line no-control-regex
), Fa = Tt(/^html$/i), Pa = Tt(/^[a-z][.\w]*(-[.\w]+)+$/i), Ot = {
  element: 1,
  attribute: 2,
  text: 3,
  cdataSection: 4,
  entityReference: 5,
  // Deprecated
  entityNode: 6,
  // Deprecated
  progressingInstruction: 7,
  comment: 8,
  document: 9,
  documentType: 10,
  documentFragment: 11,
  notation: 12
  // Deprecated
}, za = function() {
  return typeof window > "u" ? null : window;
}, Ba = function(t, n) {
  if (typeof t != "object" || typeof t.createPolicy != "function")
    return null;
  let i = null;
  const r = "data-tt-policy-suffix";
  n && n.hasAttribute(r) && (i = n.getAttribute(r));
  const o = "dompurify" + (i ? "#" + i : "");
  try {
    return t.createPolicy(o, {
      createHTML(a) {
        return a;
      },
      createScriptURL(a) {
        return a;
      }
    });
  } catch {
    return console.warn("TrustedTypes policy " + o + " could not be created."), null;
  }
}, Pi = function() {
  return {
    afterSanitizeAttributes: [],
    afterSanitizeElements: [],
    afterSanitizeShadowDOM: [],
    beforeSanitizeAttributes: [],
    beforeSanitizeElements: [],
    beforeSanitizeShadowDOM: [],
    uponSanitizeAttribute: [],
    uponSanitizeElement: [],
    uponSanitizeShadowNode: []
  };
};
function sr() {
  let e = arguments.length > 0 && arguments[0] !== void 0 ? arguments[0] : za();
  const t = (D) => sr(D);
  if (t.version = "3.4.7", t.removed = [], !e || !e.document || e.document.nodeType !== Ot.document || !e.Element)
    return t.isSupported = !1, t;
  let n = e.document;
  const i = n, r = i.currentScript;
  e.DocumentFragment;
  const o = e.HTMLTemplateElement, a = e.Node, l = e.Element, h = e.NodeFilter, s = e.NamedNodeMap;
  s === void 0 && (e.NamedNodeMap || e.MozNamedAttrMap), e.HTMLFormElement;
  const f = e.DOMParser, w = e.trustedTypes, m = l.prototype, p = Mt(m, "cloneNode"), A = Mt(m, "remove"), y = Mt(m, "nextSibling"), _ = Mt(m, "childNodes"), v = Mt(m, "parentNode"), S = Mt(m, "shadowRoot"), x = Mt(m, "attributes"), g = a && a.prototype ? Mt(a.prototype, "nodeType") : null, I = a && a.prototype ? Mt(a.prototype, "nodeName") : null;
  if (typeof o == "function") {
    const D = n.createElement("template");
    D.content && D.content.ownerDocument && (n = D.content.ownerDocument);
  }
  let C, L = "";
  const F = n, Y = F.implementation, j = F.createNodeIterator, Q = F.createDocumentFragment, at = F.getElementsByTagName, ot = i.importNode;
  let q = Pi();
  t.isSupported = typeof rr == "function" && typeof v == "function" && Y && Y.createHTMLDocument !== void 0;
  const Ht = Ra, Nt = Ia, kt = Ca, ve = Oa, se = Ma, Qt = ka, Ye = La, Nn = Pa;
  let G = Fi, K = null;
  const be = M({}, [...Oi, ...Wn, ...Vn, ...Yn, ...Mi]);
  let $ = null;
  const ae = M({}, [...ki, ...$n, ...Li, ...un]);
  let H = Object.seal(me(null, {
    tagNameCheck: {
      writable: !0,
      configurable: !1,
      enumerable: !0,
      value: null
    },
    attributeNameCheck: {
      writable: !0,
      configurable: !1,
      enumerable: !0,
      value: null
    },
    allowCustomizedBuiltInElements: {
      writable: !0,
      configurable: !1,
      enumerable: !0,
      value: !1
    }
  })), Wt = null, Vt = null;
  const St = Object.seal(me(null, {
    tagCheck: {
      writable: !0,
      configurable: !1,
      enumerable: !0,
      value: null
    },
    attributeCheck: {
      writable: !0,
      configurable: !1,
      enumerable: !0,
      value: null
    }
  }));
  let we = !0, Te = !0, $e = !1, qe = !0, Dt = !1, Jt = !0, Lt = !1, Se = !1, Yt = !1, Rt = !1, Ft = !1, pt = !1, Ae = !0, xe = !1;
  const Ke = "user-content-";
  let Ee = !0, It = !1, Pt = {}, gt = null;
  const le = M({}, ["annotation-xml", "audio", "colgroup", "desc", "foreignobject", "head", "iframe", "math", "mi", "mn", "mo", "ms", "mtext", "noembed", "noframes", "noscript", "plaintext", "script", "style", "svg", "template", "thead", "title", "video", "xmp"]);
  let Ne = null;
  const Xe = M({}, ["audio", "video", "img", "source", "image", "track"]);
  let De = null;
  const Ct = M({}, ["alt", "class", "for", "id", "label", "name", "pattern", "placeholder", "role", "summary", "title", "value", "style", "xmlns"]), zt = "http://www.w3.org/1998/Math/MathML", ce = "http://www.w3.org/2000/svg", mt = "http://www.w3.org/1999/xhtml";
  let $t = mt, ue = !1, Bt = null;
  const Dn = M({}, [zt, ce, mt], Hn);
  let At = M({}, ["mi", "mo", "mn", "ms", "mtext"]), Re = M({}, ["annotation-xml"]);
  const Rn = M({}, ["title", "style", "font", "a", "script"]);
  let te = null;
  const In = ["application/xhtml+xml", "text/html"], Cn = "text/html";
  let B = null, qt = null;
  const On = n.createElement("form"), Ze = function(u) {
    return u instanceof RegExp || u instanceof Function;
  }, Ie = function() {
    let u = arguments.length > 0 && arguments[0] !== void 0 ? arguments[0] : {};
    if (qt && qt === u)
      return;
    (!u || typeof u != "object") && (u = {}), u = st(u), te = // eslint-disable-next-line unicorn/prefer-includes
    In.indexOf(u.PARSER_MEDIA_TYPE) === -1 ? Cn : u.PARSER_MEDIA_TYPE, B = te === "application/xhtml+xml" ? Hn : Ue, K = J(u, "ALLOWED_TAGS") && lt(u.ALLOWED_TAGS) ? M({}, u.ALLOWED_TAGS, B) : be, $ = J(u, "ALLOWED_ATTR") && lt(u.ALLOWED_ATTR) ? M({}, u.ALLOWED_ATTR, B) : ae, Bt = J(u, "ALLOWED_NAMESPACES") && lt(u.ALLOWED_NAMESPACES) ? M({}, u.ALLOWED_NAMESPACES, Hn) : Dn, De = J(u, "ADD_URI_SAFE_ATTR") && lt(u.ADD_URI_SAFE_ATTR) ? M(st(Ct), u.ADD_URI_SAFE_ATTR, B) : Ct, Ne = J(u, "ADD_DATA_URI_TAGS") && lt(u.ADD_DATA_URI_TAGS) ? M(st(Xe), u.ADD_DATA_URI_TAGS, B) : Xe, gt = J(u, "FORBID_CONTENTS") && lt(u.FORBID_CONTENTS) ? M({}, u.FORBID_CONTENTS, B) : le, Wt = J(u, "FORBID_TAGS") && lt(u.FORBID_TAGS) ? M({}, u.FORBID_TAGS, B) : st({}), Vt = J(u, "FORBID_ATTR") && lt(u.FORBID_ATTR) ? M({}, u.FORBID_ATTR, B) : st({}), Pt = J(u, "USE_PROFILES") ? u.USE_PROFILES && typeof u.USE_PROFILES == "object" ? st(u.USE_PROFILES) : u.USE_PROFILES : !1, we = u.ALLOW_ARIA_ATTR !== !1, Te = u.ALLOW_DATA_ATTR !== !1, $e = u.ALLOW_UNKNOWN_PROTOCOLS || !1, qe = u.ALLOW_SELF_CLOSE_IN_ATTR !== !1, Dt = u.SAFE_FOR_TEMPLATES || !1, Jt = u.SAFE_FOR_XML !== !1, Lt = u.WHOLE_DOCUMENT || !1, Rt = u.RETURN_DOM || !1, Ft = u.RETURN_DOM_FRAGMENT || !1, pt = u.RETURN_TRUSTED_TYPE || !1, Yt = u.FORCE_BODY || !1, Ae = u.SANITIZE_DOM !== !1, xe = u.SANITIZE_NAMED_PROPS || !1, Ee = u.KEEP_CONTENT !== !1, It = u.IN_PLACE || !1, G = Ea(u.ALLOWED_URI_REGEXP) ? u.ALLOWED_URI_REGEXP : Fi, $t = typeof u.NAMESPACE == "string" ? u.NAMESPACE : mt, At = J(u, "MATHML_TEXT_INTEGRATION_POINTS") && u.MATHML_TEXT_INTEGRATION_POINTS && typeof u.MATHML_TEXT_INTEGRATION_POINTS == "object" ? st(u.MATHML_TEXT_INTEGRATION_POINTS) : M({}, ["mi", "mo", "mn", "ms", "mtext"]), Re = J(u, "HTML_INTEGRATION_POINTS") && u.HTML_INTEGRATION_POINTS && typeof u.HTML_INTEGRATION_POINTS == "object" ? st(u.HTML_INTEGRATION_POINTS) : M({}, ["annotation-xml"]);
    const T = J(u, "CUSTOM_ELEMENT_HANDLING") && u.CUSTOM_ELEMENT_HANDLING && typeof u.CUSTOM_ELEMENT_HANDLING == "object" ? st(u.CUSTOM_ELEMENT_HANDLING) : me(null);
    if (H = me(null), J(T, "tagNameCheck") && Ze(T.tagNameCheck) && (H.tagNameCheck = T.tagNameCheck), J(T, "attributeNameCheck") && Ze(T.attributeNameCheck) && (H.attributeNameCheck = T.attributeNameCheck), J(T, "allowCustomizedBuiltInElements") && typeof T.allowCustomizedBuiltInElements == "boolean" && (H.allowCustomizedBuiltInElements = T.allowCustomizedBuiltInElements), Dt && (Te = !1), Ft && (Rt = !0), Pt && (K = M({}, Mi), $ = me(null), Pt.html === !0 && (M(K, Oi), M($, ki)), Pt.svg === !0 && (M(K, Wn), M($, $n), M($, un)), Pt.svgFilters === !0 && (M(K, Vn), M($, $n), M($, un)), Pt.mathMl === !0 && (M(K, Yn), M($, Li), M($, un))), St.tagCheck = null, St.attributeCheck = null, J(u, "ADD_TAGS") && (typeof u.ADD_TAGS == "function" ? St.tagCheck = u.ADD_TAGS : lt(u.ADD_TAGS) && (K === be && (K = st(K)), M(K, u.ADD_TAGS, B))), J(u, "ADD_ATTR") && (typeof u.ADD_ATTR == "function" ? St.attributeCheck = u.ADD_ATTR : lt(u.ADD_ATTR) && ($ === ae && ($ = st($)), M($, u.ADD_ATTR, B))), J(u, "ADD_URI_SAFE_ATTR") && lt(u.ADD_URI_SAFE_ATTR) && M(De, u.ADD_URI_SAFE_ATTR, B), J(u, "FORBID_CONTENTS") && lt(u.FORBID_CONTENTS) && (gt === le && (gt = st(gt)), M(gt, u.FORBID_CONTENTS, B)), J(u, "ADD_FORBID_CONTENTS") && lt(u.ADD_FORBID_CONTENTS) && (gt === le && (gt = st(gt)), M(gt, u.ADD_FORBID_CONTENTS, B)), Ee && (K["#text"] = !0), Lt && M(K, ["html", "head", "body"]), K.table && (M(K, ["tbody"]), delete Wt.tbody), u.TRUSTED_TYPES_POLICY) {
      if (typeof u.TRUSTED_TYPES_POLICY.createHTML != "function")
        throw Pe('TRUSTED_TYPES_POLICY configuration option must provide a "createHTML" hook.');
      if (typeof u.TRUSTED_TYPES_POLICY.createScriptURL != "function")
        throw Pe('TRUSTED_TYPES_POLICY configuration option must provide a "createScriptURL" hook.');
      C = u.TRUSTED_TYPES_POLICY, L = C.createHTML("");
    } else
      C === void 0 && (C = Ba(w, r)), C !== null && typeof L == "string" && (L = C.createHTML(""));
    (q.uponSanitizeElement.length > 0 || q.uponSanitizeAttribute.length > 0) && K === be && (K = st(K)), q.uponSanitizeAttribute.length > 0 && $ === ae && ($ = st($)), ut && ut(u), qt = u;
  }, Qe = M({}, [...Wn, ...Vn, ...Na]), Ut = M({}, [...Yn, ...Da]), Mn = function(u) {
    let T = v(u);
    (!T || !T.tagName) && (T = {
      namespaceURI: $t,
      tagName: "template"
    });
    const N = Ue(u.tagName), z = Ue(T.tagName);
    return Bt[u.namespaceURI] ? u.namespaceURI === ce ? T.namespaceURI === mt ? N === "svg" : T.namespaceURI === zt ? N === "svg" && (z === "annotation-xml" || At[z]) : !!Qe[N] : u.namespaceURI === zt ? T.namespaceURI === mt ? N === "math" : T.namespaceURI === ce ? N === "math" && Re[z] : !!Ut[N] : u.namespaceURI === mt ? T.namespaceURI === ce && !Re[z] || T.namespaceURI === zt && !At[z] ? !1 : !Ut[N] && (Rn[N] || !Qe[N]) : !!(te === "application/xhtml+xml" && Bt[u.namespaceURI]) : !1;
  }, yt = function(u) {
    pe(t.removed, {
      element: u
    });
    try {
      v(u).removeChild(u);
    } catch {
      A(u);
    }
  }, jt = function(u, T) {
    try {
      pe(t.removed, {
        attribute: T.getAttributeNode(u),
        from: T
      });
    } catch {
      pe(t.removed, {
        attribute: null,
        from: T
      });
    }
    if (T.removeAttribute(u), u === "is")
      if (Rt || Ft)
        try {
          yt(T);
        } catch {
        }
      else
        try {
          T.setAttribute(u, "");
        } catch {
        }
  }, Je = function(u) {
    let T = null, N = null;
    if (Yt)
      u = "<remove></remove>" + u;
    else {
      const W = Di(u, /^[\r\n\t ]+/);
      N = W && W[0];
    }
    te === "application/xhtml+xml" && $t === mt && (u = '<html xmlns="http://www.w3.org/1999/xhtml"><head></head><body>' + u + "</body></html>");
    const z = C ? C.createHTML(u) : u;
    if ($t === mt)
      try {
        T = new f().parseFromString(z, te);
      } catch {
      }
    if (!T || !T.documentElement) {
      T = Y.createDocument($t, "template", null);
      try {
        T.documentElement.innerHTML = ue ? L : z;
      } catch {
      }
    }
    const k = T.body || T.documentElement;
    return u && N && k.insertBefore(n.createTextNode(N), k.childNodes[0] || null), $t === mt ? at.call(T, Lt ? "html" : "body")[0] : Lt ? T.documentElement : k;
  }, tn = function(u) {
    return j.call(
      u.ownerDocument || u,
      u,
      // eslint-disable-next-line no-bitwise
      h.SHOW_ELEMENT | h.SHOW_COMMENT | h.SHOW_TEXT | h.SHOW_PROCESSING_INSTRUCTION | h.SHOW_CDATA_SECTION,
      null
    );
  }, Gt = function(u) {
    u.normalize();
    const T = j.call(
      u.ownerDocument || u,
      u,
      // eslint-disable-next-line no-bitwise
      h.SHOW_TEXT | h.SHOW_COMMENT | h.SHOW_CDATA_SECTION | h.SHOW_PROCESSING_INSTRUCTION,
      null
    );
    let N = T.nextNode();
    for (; N; ) {
      let z = N.data;
      de([Ht, Nt, kt], (k) => {
        z = ge(z, k, " ");
      }), N.data = z, N = T.nextNode();
    }
  }, he = function(u) {
    const T = I ? I(u) : null;
    return typeof T != "string" || B(T) !== "form" ? !1 : typeof u.nodeName != "string" || typeof u.textContent != "string" || typeof u.removeChild != "function" || // Realm-safe NamedNodeMap detection: equality against the cached
    // prototype getter. Clobbered .attributes (e.g. <input name="attributes">)
    // makes the direct read diverge from the cached read; a clean form
    // (same-realm OR foreign-realm) has both reads pointing at the same
    // canonical NamedNodeMap.
    u.attributes !== x(u) || typeof u.removeAttribute != "function" || typeof u.setAttribute != "function" || typeof u.namespaceURI != "string" || typeof u.insertBefore != "function" || typeof u.hasChildNodes != "function" || // NodeType clobbering probe. Cached Node.prototype.nodeType getter
    // returns the integer 1 for any Element regardless of realm; direct
    // read on a clobbered form (e.g. <input name="nodeType">) returns
    // the named child element. Cheap addition — nodeType is read from
    // an internal slot, no serialization cost — and removes a residual
    // clobbering surface used by several mXSS / PI / comment branches
    // in _sanitizeElements that compare currentNode.nodeType directly.
    u.nodeType !== g(u) || // HTMLFormElement has [LegacyOverrideBuiltIns]: a descendant named
    // "childNodes" shadows the prototype getter. Direct reads of
    // form.childNodes from a clobbered form return the named child
    // instead of the real NodeList, so any walk that reads it directly
    // skips the form's real children. Compare the direct read to the
    // cached Node.prototype getter — when the form's named-property
    // getter intercepts the read, the two values differ and we flag
    // the form. This catches every clobbering child type (input,
    // select, etc.) regardless of whether the named child happens to
    // carry a numeric .length, which a typeof-based probe would miss
    // (e.g. HTMLSelectElement.length is a defined unsigned-long).
    u.childNodes !== _(u);
  }, ee = function(u) {
    if (!g || typeof u != "object" || u === null)
      return !1;
    try {
      return g(u) === Ot.documentFragment;
    } catch {
      return !1;
    }
  }, fe = function(u) {
    if (!g || typeof u != "object" || u === null)
      return !1;
    try {
      return typeof g(u) == "number";
    } catch {
      return !1;
    }
  };
  function xt(D, u, T) {
    de(D, (N) => {
      N.call(t, u, T, qt);
    });
  }
  const en = function(u) {
    let T = null;
    if (xt(q.beforeSanitizeElements, u, null), he(u))
      return yt(u), !0;
    const N = B(u.nodeName);
    if (xt(q.uponSanitizeElement, u, {
      tagName: N,
      allowedTags: K
    }), Jt && u.hasChildNodes() && !fe(u.firstElementChild) && rt(/<[/\w!]/g, u.innerHTML) && rt(/<[/\w!]/g, u.textContent) || Jt && u.namespaceURI === mt && N === "style" && fe(u.firstElementChild) || u.nodeType === Ot.progressingInstruction || Jt && u.nodeType === Ot.comment && rt(/<[/\w]/g, u.data))
      return yt(u), !0;
    if (Wt[N] || !(St.tagCheck instanceof Function && St.tagCheck(N)) && !K[N]) {
      if (!Wt[N] && rn(N) && (H.tagNameCheck instanceof RegExp && rt(H.tagNameCheck, N) || H.tagNameCheck instanceof Function && H.tagNameCheck(N)))
        return !1;
      if (Ee && !gt[N]) {
        const k = v(u), W = _(u);
        if (W && k) {
          const ft = W.length;
          for (let bt = ft - 1; bt >= 0; --bt) {
            const _t = p(W[bt], !0);
            k.insertBefore(_t, y(u));
          }
        }
      }
      return yt(u), !0;
    }
    return (g ? g(u) : u.nodeType) === Ot.element && !Mn(u) || (N === "noscript" || N === "noembed" || N === "noframes") && rt(/<\/no(script|embed|frames)/i, u.innerHTML) ? (yt(u), !0) : (Dt && u.nodeType === Ot.text && (T = u.textContent, de([Ht, Nt, kt], (k) => {
      T = ge(T, k, " ");
    }), u.textContent !== T && (pe(t.removed, {
      element: u.cloneNode()
    }), u.textContent = T)), xt(q.afterSanitizeElements, u, null), !1);
  }, nn = function(u, T, N) {
    if (Vt[T] || Ae && (T === "id" || T === "name") && (N in n || N in On))
      return !1;
    const z = $[T] || St.attributeCheck instanceof Function && St.attributeCheck(T, u);
    if (!(Te && !Vt[T] && rt(ve, T))) {
      if (!(we && rt(se, T))) {
        if (!z || Vt[T]) {
          if (
            // First condition does a very basic check if a) it's basically a valid custom element tagname AND
            // b) if the tagName passes whatever the user has configured for CUSTOM_ELEMENT_HANDLING.tagNameCheck
            // and c) if the attribute name passes whatever the user has configured for CUSTOM_ELEMENT_HANDLING.attributeNameCheck
            !(rn(u) && (H.tagNameCheck instanceof RegExp && rt(H.tagNameCheck, u) || H.tagNameCheck instanceof Function && H.tagNameCheck(u)) && (H.attributeNameCheck instanceof RegExp && rt(H.attributeNameCheck, T) || H.attributeNameCheck instanceof Function && H.attributeNameCheck(T, u)) || // Alternative, second condition checks if it's an `is`-attribute, AND
            // the value passes whatever the user has configured for CUSTOM_ELEMENT_HANDLING.tagNameCheck
            T === "is" && H.allowCustomizedBuiltInElements && (H.tagNameCheck instanceof RegExp && rt(H.tagNameCheck, N) || H.tagNameCheck instanceof Function && H.tagNameCheck(N)))
          ) return !1;
        } else if (!De[T]) {
          if (!rt(G, ge(N, Ye, ""))) {
            if (!((T === "src" || T === "xlink:href" || T === "href") && u !== "script" && Ri(N, "data:") === 0 && Ne[u])) {
              if (!($e && !rt(Qt, ge(N, Ye, "")))) {
                if (N)
                  return !1;
              }
            }
          }
        }
      }
    }
    return !0;
  }, Ce = M({}, ["annotation-xml", "color-profile", "font-face", "font-face-format", "font-face-name", "font-face-src", "font-face-uri", "missing-glyph"]), rn = function(u) {
    return !Ce[Ue(u)] && rt(Nn, u);
  }, ne = function(u) {
    xt(q.beforeSanitizeAttributes, u, null);
    const T = u.attributes;
    if (!T || he(u))
      return;
    const N = {
      attrName: "",
      attrValue: "",
      keepAttr: !0,
      allowedAttributes: $,
      forceKeepAttr: void 0
    };
    let z = T.length;
    for (; z--; ) {
      const k = T[z], W = k.name, ft = k.namespaceURI, bt = k.value, _t = B(W), Oe = bt;
      let tt = W === "value" ? Oe : ba(Oe);
      if (N.attrName = _t, N.attrValue = tt, N.keepAttr = !0, N.forceKeepAttr = void 0, xt(q.uponSanitizeAttribute, u, N), tt = N.attrValue, xe && (_t === "id" || _t === "name") && Ri(tt, Ke) !== 0 && (jt(W, u), tt = Ke + tt), Jt && rt(/((--!?|])>)|<\/(style|script|title|xmp|textarea|noscript|iframe|noembed|noframes)/i, tt)) {
        jt(W, u);
        continue;
      }
      if (_t === "attributename" && Di(tt, "href")) {
        jt(W, u);
        continue;
      }
      if (N.forceKeepAttr)
        continue;
      if (!N.keepAttr) {
        jt(W, u);
        continue;
      }
      if (!qe && rt(/\/>/i, tt)) {
        jt(W, u);
        continue;
      }
      Dt && de([Ht, Nt, kt], (sn) => {
        tt = ge(tt, sn, " ");
      });
      const on = B(u.nodeName);
      if (!nn(on, _t, tt)) {
        jt(W, u);
        continue;
      }
      if (C && typeof w == "object" && typeof w.getAttributeType == "function" && !ft)
        switch (w.getAttributeType(on, _t)) {
          case "TrustedHTML": {
            tt = C.createHTML(tt);
            break;
          }
          case "TrustedScriptURL": {
            tt = C.createScriptURL(tt);
            break;
          }
        }
      if (tt !== Oe)
        try {
          ft ? u.setAttributeNS(ft, W, tt) : u.setAttribute(W, tt), he(u) ? yt(u) : Ni(t.removed);
        } catch {
          jt(W, u);
        }
    }
    xt(q.afterSanitizeAttributes, u, null);
  }, Kt = function(u) {
    let T = null;
    const N = tn(u);
    for (xt(q.beforeSanitizeShadowDOM, u, null); T = N.nextNode(); )
      if (xt(q.uponSanitizeShadowNode, T, null), en(T), ne(T), ee(T.content) && Kt(T.content), (g ? g(T) : T.nodeType) === Ot.element) {
        const k = S ? S(T) : T.shadowRoot;
        ee(k) && (Xt(k), Kt(k));
      }
    xt(q.afterSanitizeShadowDOM, u, null);
  }, Xt = function(u) {
    const T = g ? g(u) : u.nodeType;
    if (T === Ot.element) {
      const k = S ? S(u) : u.shadowRoot;
      ee(k) && (Xt(k), Kt(k));
    }
    const N = _ ? _(u) : u.childNodes;
    if (!N)
      return;
    const z = [];
    de(N, (k) => {
      pe(z, k);
    });
    for (const k of z)
      Xt(k);
    if (T === Ot.element) {
      const k = I ? I(u) : null;
      if (typeof k == "string" && B(k) === "template") {
        const W = u.content;
        ee(W) && Xt(W);
      }
    }
  };
  return t.sanitize = function(D) {
    let u = arguments.length > 1 && arguments[1] !== void 0 ? arguments[1] : {}, T = null, N = null, z = null, k = null;
    if (ue = !D, ue && (D = "<!-->"), typeof D != "string" && !fe(D) && (D = xa(D), typeof D != "string"))
      throw Pe("dirty is not a string, aborting");
    if (!t.isSupported)
      return D;
    if (Se || Ie(u), t.removed = [], typeof D == "string" && (It = !1), It) {
      const bt = I ? I(D) : D.nodeName;
      if (typeof bt == "string") {
        const _t = B(bt);
        if (!K[_t] || Wt[_t])
          throw Pe("root node is forbidden and cannot be sanitized in-place");
      }
      if (he(D))
        throw Pe("root node is clobbered and cannot be sanitized in-place");
      Xt(D);
    } else if (fe(D))
      T = Je("<!---->"), N = T.ownerDocument.importNode(D, !0), N.nodeType === Ot.element && N.nodeName === "BODY" || N.nodeName === "HTML" ? T = N : T.appendChild(N), Xt(N);
    else {
      if (!Rt && !Dt && !Lt && // eslint-disable-next-line unicorn/prefer-includes
      D.indexOf("<") === -1)
        return C && pt ? C.createHTML(D) : D;
      if (T = Je(D), !T)
        return Rt ? null : pt ? L : "";
    }
    T && Yt && yt(T.firstChild);
    const W = tn(It ? D : T);
    for (; z = W.nextNode(); )
      en(z), ne(z), ee(z.content) && Kt(z.content);
    if (It)
      return Dt && Gt(D), D;
    if (Rt) {
      if (Dt && Gt(T), Ft)
        for (k = Q.call(T.ownerDocument); T.firstChild; )
          k.appendChild(T.firstChild);
      else
        k = T;
      return ($.shadowroot || $.shadowrootmode) && (k = ot.call(i, k, !0)), k;
    }
    let ft = Lt ? T.outerHTML : T.innerHTML;
    return Lt && K["!doctype"] && T.ownerDocument && T.ownerDocument.doctype && T.ownerDocument.doctype.name && rt(Fa, T.ownerDocument.doctype.name) && (ft = "<!DOCTYPE " + T.ownerDocument.doctype.name + `>
` + ft), Dt && de([Ht, Nt, kt], (bt) => {
      ft = ge(ft, bt, " ");
    }), C && pt ? C.createHTML(ft) : ft;
  }, t.setConfig = function() {
    let D = arguments.length > 0 && arguments[0] !== void 0 ? arguments[0] : {};
    Ie(D), Se = !0;
  }, t.clearConfig = function() {
    qt = null, Se = !1;
  }, t.isValidAttribute = function(D, u, T) {
    qt || Ie({});
    const N = B(D), z = B(u);
    return nn(N, z, T);
  }, t.addHook = function(D, u) {
    typeof u == "function" && pe(q[D], u);
  }, t.removeHook = function(D, u) {
    if (u !== void 0) {
      const T = _a(q[D], u);
      return T === -1 ? void 0 : va(q[D], T, 1)[0];
    }
    return Ni(q[D]);
  }, t.removeHooks = function(D) {
    q[D] = [];
  }, t.removeAllHooks = function() {
    q = Pi();
  }, t;
}
sr();
function ar(e = 8, t = "id-") {
  const n = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", i = n + "0123456789-_";
  let r = n.charAt(Math.floor(Math.random() * n.length));
  for (let o = 1; o < e; o++)
    r += i.charAt(Math.floor(Math.random() * i.length));
  return `${t}${r}`;
}
let lr = class cr {
  /**
   * Create a new Node instance.
   * @param id - Unique identifier for the node
   * @param data - Optional data payload associated with the node
   */
  constructor(t, n, i, r = ar(), o = []) {
    E(this, "id");
    E(this, "data");
    E(this, "children");
    E(this, "style");
    E(this, "edgesOut");
    E(this, "edgesIn");
    E(this, "defaultCircleRadius", 10);
    // Layout/physics properties
    E(this, "x");
    E(this, "y");
    E(this, "vx");
    E(this, "vy");
    E(this, "fx");
    E(this, "fy");
    E(this, "weight");
    E(this, "frozen");
    E(this, "visible");
    E(this, "expanded");
    /** True if this node is a child within a collapsed cluster */
    E(this, "isChild");
    E(this, "childrenDepth");
    /** True if this node has child nodes */
    E(this, "isParent");
    /** Reference to the parent cluster node (if this node is a child) */
    E(this, "parentNode");
    /**
     * Reference to the main graph node when this node is a clone in a subgraph.
     * Used for syncing position updates from subgraph back to main graph.
     */
    E(this, "_original_object");
    /**
     * Reference to the deepest sub graph node.
     * Used for checking state of this node in its subgraph
     */
    E(this, "_deepest_node_clone");
    /** The subgraph graph instance created when expanding this node */
    E(this, "_subgraph");
    E(this, "_circleRadius", this.defaultCircleRadius);
    E(this, "_circleRadiusCollapsed", this.defaultCircleRadius);
    E(this, "_dirty");
    E(this, "domID");
    this.id = t, this.domID = r, this.data = n ?? {}, this.style = i ?? {}, this.children = [], this.isParent = !1, this.setChildren(o), this._dirty = !0, this.frozen = !1, this.visible = !0, this.expanded = !1, this.isChild = !1, this.childrenDepth = 0, this.edgesOut = /* @__PURE__ */ new Set(), this.edgesIn = /* @__PURE__ */ new Set();
  }
  /**
   * Get the node's data.
   */
  getData() {
    return this.data;
  }
  /**
   * Update the node's data.
   * @param newData - New data to set
   */
  setData(t) {
    this.data = t, this.markDirty();
  }
  /**
   * Merge partial data into the current node data.
   * Useful for updating only parts of the data.
   * @param partialData - Partial data object to merge
   */
  updateData(t) {
    this.data = { ...this.data, ...t }, this.markDirty();
  }
  /**
   * @private
   */
  registerEdgeOut(t) {
    this.edgesOut.add(t);
  }
  /**
   * @private
   */
  registerEdgeIn(t) {
    this.edgesIn.add(t);
  }
  /**
   * @private
   */
  emptyEdges() {
    this.edgesOut.clear(), this.edgesIn.clear();
  }
  getConnectedNodes() {
    return [...this.edgesOut].map((t) => t.to);
  }
  getConnectingNodes() {
    return [...this.edgesIn].map((t) => t.from);
  }
  getEdgesOut() {
    return [...this.edgesOut];
  }
  getEdgesIn() {
    return [...this.edgesIn];
  }
  /**
   * Get the node's data.
   */
  getStyle() {
    return this.style;
  }
  /**
   * Update the node's data.
   * @param newStyle - New data to set
   */
  setStyle(t) {
    this.style = t, this.markDirty();
  }
  /**
   * Merge partial data into the current node data.
   * Useful for updating only parts of the data.
   * @param partialStyle - Partial data object to merge
   */
  updateStyle(t) {
    this.style = { ...this.style, ...t }, this.markDirty();
  }
  getGraphElement() {
    return document ? document.getElementById(`node-${this.domID}`) : null;
  }
  /**
   * Convert node to a simple JSON object representation.
   * @param dataOnly - default: false
   */
  toDict(t = !1) {
    const n = {
      id: this.id,
      data: this.data,
      style: this.style,
      weight: this.weight
      // expanded: this.expanded,
    };
    return t || (n.x = this.x, n.y = this.y, n.vx = this.vx, n.vy = this.vy, n.fx = this.fx, n.fy = this.fy), this.hasChildren() && (n.children = this.children.map((i) => i.toDict(t))), n;
  }
  /** Structured-cloneable payload for the simulation worker (no live parent/children/_subgraph refs, unlike `clone()`). */
  toSimulationDTO() {
    return {
      id: this.id,
      data: this.data,
      style: this.style,
      weight: this.weight,
      _circleRadius: this._circleRadius,
      x: this.x,
      y: this.y,
      vx: this.vx,
      vy: this.vy,
      fx: this.fx,
      fy: this.fy
    };
  }
  clone() {
    const t = { ...this.data }, n = { ...this.style }, i = new cr(this.id, t, n);
    return i.x = this.x, i.y = this.y, i.vx = this.vx, i.vy = this.vy, i.fx = this.fx, i.fy = this.fy, i.weight = this.weight, i.frozen = this.frozen, i.visible = this.visible, i.expanded = this.expanded, i.isChild = this.isChild, i.childrenDepth = this.childrenDepth, i.isParent = this.isParent, i.parentNode = this.parentNode, i._circleRadius = this._circleRadius, i.children = this.children.map((r) => r.clone()), i;
  }
  /**
   * @private
   */
  markDirty() {
    this._dirty = !0;
  }
  /**
   * @private
   */
  clearDirty() {
    this._dirty = !1;
  }
  /**
   * @private
   */
  isDirty() {
    return this._dirty;
  }
  freeze() {
    this.frozen = !0, this.fx = this.x, this.fy = this.y;
  }
  unfreeze() {
    this.frozen = !1, this.fx = void 0, this.fy = void 0;
  }
  toggleVisibility(t) {
    t ? this.show() : this.hide(), this.markDirty();
  }
  show() {
    this.visible = !0;
  }
  hide() {
    this.visible = !1;
  }
  toggleExpand(t) {
    t === void 0 ? this.expanded ? this.collapse() : this.expand() : t ? this.expand() : this.collapse(), this.markDirty();
  }
  expand() {
    this.expanded = !0, this._original_object && (this._original_object.expanded = !0);
  }
  collapse() {
    this.expanded = !1, this._original_object && (this._original_object.expanded = !1);
  }
  degree() {
    return this.edgesOut.size + this.edgesIn.size;
  }
  setCircleRadius(t) {
    this._circleRadius = t;
  }
  getCircleRadius() {
    return this._circleRadius;
  }
  setCircleRadiusCollapsed(t) {
    this._circleRadiusCollapsed = t;
  }
  getCircleRadiusCollapsed() {
    return this._circleRadiusCollapsed;
  }
  setChildren(t) {
    this.children = t, this.hasChildren() ? this.isParent = !0 : this.isParent = !1;
  }
  hasChildren() {
    return this.children.length > 0;
  }
  markAsChild(t, n) {
    this.isChild = !0, this.childrenDepth = n, this.parentNode = t;
  }
  markAsParent() {
    this.isParent = !0;
  }
  /**
   * Sets the subgraph instance (when opening a cluster).
   * @private
   */
  setSubgraph(t) {
    this._subgraph = t;
  }
  /**
   * Gets the subgraph instance created from this node.
   * Returns undefined if this node didn't created a subgraph.
   * @private
   */
  getSubgraph() {
    return this._subgraph;
  }
  /**
   * Sets a reference to the original node from the main graph.
   * Used when this node is a clone in a subgraph to enable position syncing.
   * @private
   */
  setOriginalObject(t) {
    this._original_object = t;
  }
  /**
   * Gets the reference to the original node from the main graph.
   * Returns undefined if this is not a subgraph clone.
   * @private
   */
  getOriginalObject() {
    return this._original_object;
  }
  /**
   * Sets a reference to the original node from the main graph.
   * Used when this node is a clone in a subgraph to enable position syncing.
   * @private
   */
  setDeepestNodeClone(t) {
    this._deepest_node_clone = t;
  }
  /**
   * Gets the reference to the original node from the main graph.
   * Returns undefined if this is not a subgraph clone.
   * @private
   */
  getDeepestNodeClone() {
    return this._deepest_node_clone;
  }
};
class xn {
  /**
   * Create a new Edge instance.
   * @param id - Unique identifier for the edge
   * @param from - Source node
   * @param to - Target node
   * @param data - Optional data payload for the edge
   * @param style - Optional style for the edge
   */
  constructor(t, n, i, r, o, a = null, l) {
    E(this, "id");
    E(this, "from");
    E(this, "to");
    E(this, "directed");
    E(this, "data");
    E(this, "style");
    E(this, "visible");
    /** True if this is a synthetic edge (placeholder for collapsed cluster child) */
    E(this, "isSynthetic");
    /**
     * True for the subclass of synthetic edges that stand in for a real edge whose
     * *both* endpoints are children of different clusters. Unlike the external→cluster
     * synthetic edges, these are resolved as a set (one per collapse state) by
     * {@link ClusterDrawer.resolveCrossClusterEdges} rather than the per-node toggle.
     */
    E(this, "isCrossCluster");
    /** The actual child node this synthetic edge points to (for expansion logic) */
    E(this, "syntheticTerminalNode");
    /** For a cross-cluster synthetic edge: the real child the `from` side stands in for. */
    E(this, "syntheticSourceNode");
    E(this, "_original_object");
    E(this, "_subgraphFromNode");
    E(this, "_subgraphToNode");
    E(this, "_dirty");
    E(this, "domID");
    this.id = t, this.domID = ar(), this.from = n, this.to = i, this.directed = a, this.data = r ?? {}, this.style = o ?? {}, this.visible = !0, this._dirty = !0, this.isSynthetic = l !== void 0, this.syntheticTerminalNode = l, this.from.registerEdgeOut(this), this.to.registerEdgeIn(this);
  }
  /** Required by d3-force */
  get source() {
    return this.from;
  }
  get target() {
    return this.to;
  }
  /**
   * Get the edge's data.
   */
  getData() {
    return this.data;
  }
  /**
   * Update the edge's data.
   * @param newData - New data to set
   */
  setData(t) {
    this.data = t, this.markDirty();
  }
  /**
   * Merge partial data into the current edge data.
   * @param partialData - Partial data object to merge
   */
  updateData(t) {
    this.data = { ...this.data, ...t }, this.markDirty();
  }
  /**
   * Get the edge's style.
   */
  getStyle() {
    return this.style;
  }
  /**
   * Get the edge's style.
   */
  getEdgeStyle() {
    var t;
    return ((t = this.style) == null ? void 0 : t.edge) ?? {};
  }
  /**
   * Get the edge's label style if available.
   */
  getLabelStyle() {
    var t;
    return ((t = this.style) == null ? void 0 : t.label) ?? {};
  }
  /**
   * Update the edge's style.
   * @param newStyle - New style to set
   */
  setStyle(t) {
    this.style = t, this.markDirty();
  }
  /**
   * Merge partial style into the current edge style.
   * Useful for updating only parts of the style.
   * @param partialStyle - Partial style object to merge
   */
  updateStyle(t) {
    const n = this.style, i = t;
    this.style = {
      ...n,
      ...i,
      edge: { ...n.edge, ...i.edge },
      label: { ...n.label, ...i.label }
    }, this.markDirty();
  }
  getGraphElement() {
    return document ? document.getElementById(`edge-${this.domID}`) : null;
  }
  setFrom(t) {
    this.from = t;
  }
  setTo(t) {
    this.to = t;
  }
  /**
   * Convert edge to a simple JSON object representation.
   */
  toDict() {
    return {
      id: this.id,
      from: this.from.id,
      to: this.to.id,
      data: this.data,
      style: this.style
    };
  }
  /** Structured-cloneable payload for the simulation worker; endpoints reduced to ids, keeps `directed`. */
  toSimulationDTO() {
    return {
      id: this.id,
      from: { id: this.from.id },
      to: { id: this.to.id },
      data: this.data,
      style: this.style,
      directed: this.directed
    };
  }
  clone() {
    const t = { ...this.data }, n = { ...this.style }, i = new xn(
      this.id,
      this.from.clone(),
      this.to.clone(),
      t,
      n,
      this.directed
    );
    return i.visible = this.visible, i;
  }
  markDirty() {
    this._dirty = !0;
  }
  clearDirty() {
    this._dirty = !1;
  }
  isDirty() {
    return this._dirty;
  }
  toggleVisibility(t) {
    t ? this.show() : this.hide(), this.markDirty();
  }
  show() {
    this.visible = !0;
  }
  hide() {
    this.visible = !1;
  }
  /**
   * Sets a reference to the original node from the main graph.
   * Used when this node is a clone in a subgraph to enable position syncing.
   * @private
   */
  setOriginalObject(t) {
    this._original_object = t;
  }
  /**
   * Gets the reference to the original node from the main graph.
   * Returns undefined if this is not a subgraph clone.
   * @private
   */
  getOriginalObject() {
    return this._original_object;
  }
  /**
   * Sets a reference to the subgraph node from the main graph.
   * Used when the FROM node has a clone in a subgraph
   * @private
   */
  setSubgraphFromNode(t) {
    this._subgraphFromNode = t;
  }
  /**
   * Sets a reference to the subgraph node from the main graph.
   * Used when the TO node has a clone in a subgraph
   * @private
   */
  setSubgraphToNode(t) {
    this._subgraphToNode = t;
  }
  /**
   * Gets the reference to the subgraph node from the main graph.
   * @private
   */
  getSubgraphFromNode() {
    return this._subgraphFromNode;
  }
  /**
   * Gets the reference to the subgraph node from the main graph.
   * @private
   */
  getSubgraphToNode() {
    return this._subgraphToNode;
  }
}
const ur = 'var bl=Object.defineProperty;var Sl=(Jt,Nt,le)=>Nt in Jt?bl(Jt,Nt,{enumerable:!0,configurable:!0,writable:!0,value:le}):Jt[Nt]=le;var x=(Jt,Nt,le)=>Sl(Jt,typeof Nt!="symbol"?Nt+"":Nt,le);(function(){"use strict";function Jt(e){const t=+this._x.call(null,e),n=+this._y.call(null,e);return Nt(this.cover(t,n),t,n,e)}function Nt(e,t,n,i){if(isNaN(t)||isNaN(n))return e;var r,o=e._root,a={data:i},l=e._x0,h=e._y0,s=e._x1,f=e._y1,b,m,p,A,y,_,T,v;if(!o)return e._root=a,e;for(;o.length;)if((y=t>=(b=(l+s)/2))?l=b:s=b,(_=n>=(m=(h+f)/2))?h=m:f=m,r=o,!(o=o[T=_<<1|y]))return r[T]=a,e;if(p=+e._x.call(null,o.data),A=+e._y.call(null,o.data),t===p&&n===A)return a.next=o,r?r[T]=a:e._root=a,e;do r=r?r[T]=new Array(4):e._root=new Array(4),(y=t>=(b=(l+s)/2))?l=b:s=b,(_=n>=(m=(h+f)/2))?h=m:f=m;while((T=_<<1|y)===(v=(A>=m)<<1|p>=b));return r[v]=o,r[T]=a,e}function le(e){var t,n,i=e.length,r,o,a=new Array(i),l=new Array(i),h=1/0,s=1/0,f=-1/0,b=-1/0;for(n=0;n<i;++n)isNaN(r=+this._x.call(null,t=e[n]))||isNaN(o=+this._y.call(null,t))||(a[n]=r,l[n]=o,r<h&&(h=r),r>f&&(f=r),o<s&&(s=o),o>b&&(b=o));if(h>f||s>b)return this;for(this.cover(h,s).cover(f,b),n=0;n<i;++n)Nt(this,a[n],l[n],e[n]);return this}function dr(e,t){if(isNaN(e=+e)||isNaN(t=+t))return this;var n=this._x0,i=this._y0,r=this._x1,o=this._y1;if(isNaN(n))r=(n=Math.floor(e))+1,o=(i=Math.floor(t))+1;else{for(var a=r-n||1,l=this._root,h,s;n>e||e>=r||i>t||t>=o;)switch(s=(t<i)<<1|e<n,h=new Array(4),h[s]=l,l=h,a*=2,s){case 0:r=n+a,o=i+a;break;case 1:n=r-a,o=i+a;break;case 2:r=n+a,i=o-a;break;case 3:n=r-a,i=o-a;break}this._root&&this._root.length&&(this._root=l)}return this._x0=n,this._y0=i,this._x1=r,this._y1=o,this}function pr(){var e=[];return this.visit(function(t){if(!t.length)do e.push(t.data);while(t=t.next)}),e}function gr(e){return arguments.length?this.cover(+e[0][0],+e[0][1]).cover(+e[1][0],+e[1][1]):isNaN(this._x0)?void 0:[[this._x0,this._y0],[this._x1,this._y1]]}function at(e,t,n,i,r){this.node=e,this.x0=t,this.y0=n,this.x1=i,this.y1=r}function mr(e,t,n){var i,r=this._x0,o=this._y0,a,l,h,s,f=this._x1,b=this._y1,m=[],p=this._root,A,y;for(p&&m.push(new at(p,r,o,f,b)),n==null?n=1/0:(r=e-n,o=t-n,f=e+n,b=t+n,n*=n);A=m.pop();)if(!(!(p=A.node)||(a=A.x0)>f||(l=A.y0)>b||(h=A.x1)<r||(s=A.y1)<o))if(p.length){var _=(a+h)/2,T=(l+s)/2;m.push(new at(p[3],_,T,h,s),new at(p[2],a,T,_,s),new at(p[1],_,l,h,T),new at(p[0],a,l,_,T)),(y=(t>=T)<<1|e>=_)&&(A=m[m.length-1],m[m.length-1]=m[m.length-1-y],m[m.length-1-y]=A)}else{var v=e-+this._x.call(null,p.data),E=t-+this._y.call(null,p.data),g=v*v+E*E;if(g<n){var R=Math.sqrt(n=g);r=e-R,o=t-R,f=e+R,b=t+R,i=p.data}}return i}function yr(e){if(isNaN(f=+this._x.call(null,e))||isNaN(b=+this._y.call(null,e)))return this;var t,n=this._root,i,r,o,a=this._x0,l=this._y0,h=this._x1,s=this._y1,f,b,m,p,A,y,_,T;if(!n)return this;if(n.length)for(;;){if((A=f>=(m=(a+h)/2))?a=m:h=m,(y=b>=(p=(l+s)/2))?l=p:s=p,t=n,!(n=n[_=y<<1|A]))return this;if(!n.length)break;(t[_+1&3]||t[_+2&3]||t[_+3&3])&&(i=t,T=_)}for(;n.data!==e;)if(r=n,!(n=n.next))return this;return(o=n.next)&&delete n.next,r?(o?r.next=o:delete r.next,this):t?(o?t[_]=o:delete t[_],(n=t[0]||t[1]||t[2]||t[3])&&n===(t[3]||t[2]||t[1]||t[0])&&!n.length&&(i?i[T]=n:this._root=n),this):(this._root=o,this)}function _r(e){for(var t=0,n=e.length;t<n;++t)this.remove(e[t]);return this}function Tr(){return this._root}function wr(){var e=0;return this.visit(function(t){if(!t.length)do++e;while(t=t.next)}),e}function br(e){var t=[],n,i=this._root,r,o,a,l,h;for(i&&t.push(new at(i,this._x0,this._y0,this._x1,this._y1));n=t.pop();)if(!e(i=n.node,o=n.x0,a=n.y0,l=n.x1,h=n.y1)&&i.length){var s=(o+l)/2,f=(a+h)/2;(r=i[3])&&t.push(new at(r,s,f,l,h)),(r=i[2])&&t.push(new at(r,o,f,s,h)),(r=i[1])&&t.push(new at(r,s,a,l,f)),(r=i[0])&&t.push(new at(r,o,a,s,f))}return this}function Sr(e){var t=[],n=[],i;for(this._root&&t.push(new at(this._root,this._x0,this._y0,this._x1,this._y1));i=t.pop();){var r=i.node;if(r.length){var o,a=i.x0,l=i.y0,h=i.x1,s=i.y1,f=(a+h)/2,b=(l+s)/2;(o=r[0])&&t.push(new at(o,a,l,f,b)),(o=r[1])&&t.push(new at(o,f,l,h,b)),(o=r[2])&&t.push(new at(o,a,b,f,s)),(o=r[3])&&t.push(new at(o,f,b,h,s))}n.push(i)}for(;i=n.pop();)e(i.node,i.x0,i.y0,i.x1,i.y1);return this}function vr(e){return e[0]}function Ar(e){return arguments.length?(this._x=e,this):this._x}function Er(e){return e[1]}function xr(e){return arguments.length?(this._y=e,this):this._y}function Rn(e,t,n){var i=new Cn(t??vr,n??Er,NaN,NaN,NaN,NaN);return e==null?i:i.addAll(e)}function Cn(e,t,n,i,r,o){this._x=e,this._y=t,this._x0=n,this._y0=i,this._x1=r,this._y1=o,this._root=void 0}function ui(e){for(var t={data:e.data},n=t;e=e.next;)n=n.next={data:e.data};return t}var lt=Rn.prototype=Cn.prototype;lt.copy=function(){var e=new Cn(this._x,this._y,this._x0,this._y0,this._x1,this._y1),t=this._root,n,i;if(!t)return e;if(!t.length)return e._root=ui(t),e;for(n=[{source:t,target:e._root=new Array(4)}];t=n.pop();)for(var r=0;r<4;++r)(i=t.source[r])&&(i.length?n.push({source:i,target:t.target[r]=new Array(4)}):t.target[r]=ui(i));return e},lt.add=Jt,lt.addAll=le,lt.cover=dr,lt.data=pr,lt.extent=gr,lt.find=mr,lt.remove=yr,lt.removeAll=_r,lt.root=Tr,lt.size=wr,lt.visit=br,lt.visitAfter=Sr,lt.x=Ar,lt.y=xr;function nt(e){return function(){return e}}function Wt(e){return(e()-.5)*1e-6}function Nr(e){return e.x+e.vx}function Dr(e){return e.y+e.vy}function Ir(e){var t,n,i,r=1,o=1;typeof e!="function"&&(e=nt(e==null?1:+e));function a(){for(var s,f=t.length,b,m,p,A,y,_,T=0;T<o;++T)for(b=Rn(t,Nr,Dr).visitAfter(l),s=0;s<f;++s)m=t[s],y=n[m.index],_=y*y,p=m.x+m.vx,A=m.y+m.vy,b.visit(v);function v(E,g,R,C,k){var F=E.data,$=E.r,G=y+$;if(F){if(F.index>m.index){var Q=p-F.x-F.vx,ht=A-F.y-F.vy,st=Q*Q+ht*ht;st<G*G&&(Q===0&&(Q=Wt(i),st+=Q*Q),ht===0&&(ht=Wt(i),st+=ht*ht),st=(G-(st=Math.sqrt(st)))/st*r,m.vx+=(Q*=st)*(G=($*=$)/(_+$)),m.vy+=(ht*=st)*G,F.vx-=Q*(G=1-G),F.vy-=ht*G)}return}return g>p+G||C<p-G||R>A+G||k<A-G}}function l(s){if(s.data)return s.r=n[s.data.index];for(var f=s.r=0;f<4;++f)s[f]&&s[f].r>s.r&&(s.r=s[f].r)}function h(){if(t){var s,f=t.length,b;for(n=new Array(f),s=0;s<f;++s)b=t[s],n[b.index]=+e(b,s,t)}}return a.initialize=function(s,f){t=s,i=f,h()},a.iterations=function(s){return arguments.length?(o=+s,a):o},a.strength=function(s){return arguments.length?(r=+s,a):r},a.radius=function(s){return arguments.length?(e=typeof s=="function"?s:nt(+s),h(),a):e},a}function Rr(e){return e.index}function hi(e,t){var n=e.get(t);if(!n)throw new Error("node not found: "+t);return n}function Cr(e){var t=Rr,n=b,i,r=nt(30),o,a,l,h,s,f=1;e==null&&(e=[]);function b(_){return 1/Math.min(l[_.source.index],l[_.target.index])}function m(_){for(var T=0,v=e.length;T<f;++T)for(var E=0,g,R,C,k,F,$,G;E<v;++E)g=e[E],R=g.source,C=g.target,k=C.x+C.vx-R.x-R.vx||Wt(s),F=C.y+C.vy-R.y-R.vy||Wt(s),$=Math.sqrt(k*k+F*F),$=($-o[E])/$*_*i[E],k*=$,F*=$,C.vx-=k*(G=h[E]),C.vy-=F*G,R.vx+=k*(G=1-G),R.vy+=F*G}function p(){if(a){var _,T=a.length,v=e.length,E=new Map(a.map((R,C)=>[t(R,C,a),R])),g;for(_=0,l=new Array(T);_<v;++_)g=e[_],g.index=_,typeof g.source!="object"&&(g.source=hi(E,g.source)),typeof g.target!="object"&&(g.target=hi(E,g.target)),l[g.source.index]=(l[g.source.index]||0)+1,l[g.target.index]=(l[g.target.index]||0)+1;for(_=0,h=new Array(v);_<v;++_)g=e[_],h[_]=l[g.source.index]/(l[g.source.index]+l[g.target.index]);i=new Array(v),A(),o=new Array(v),y()}}function A(){if(a)for(var _=0,T=e.length;_<T;++_)i[_]=+n(e[_],_,e)}function y(){if(a)for(var _=0,T=e.length;_<T;++_)o[_]=+r(e[_],_,e)}return m.initialize=function(_,T){a=_,s=T,p()},m.links=function(_){return arguments.length?(e=_,p(),m):e},m.id=function(_){return arguments.length?(t=_,m):t},m.iterations=function(_){return arguments.length?(f=+_,m):f},m.strength=function(_){return arguments.length?(n=typeof _=="function"?_:nt(+_),A(),m):n},m.distance=function(_){return arguments.length?(r=typeof _=="function"?_:nt(+_),y(),m):r},m}var Or={value:()=>{}};function On(){for(var e=0,t=arguments.length,n={},i;e<t;++e){if(!(i=arguments[e]+"")||i in n||/[\\s.]/.test(i))throw new Error("illegal type: "+i);n[i]=[]}return new Xe(n)}function Xe(e){this._=e}function Mr(e,t){return e.trim().split(/^|\\s+/).map(function(n){var i="",r=n.indexOf(".");if(r>=0&&(i=n.slice(r+1),n=n.slice(0,r)),n&&!t.hasOwnProperty(n))throw new Error("unknown type: "+n);return{type:n,name:i}})}Xe.prototype=On.prototype={constructor:Xe,on:function(e,t){var n=this._,i=Mr(e+"",n),r,o=-1,a=i.length;if(arguments.length<2){for(;++o<a;)if((r=(e=i[o]).type)&&(r=Lr(n[r],e.name)))return r;return}if(t!=null&&typeof t!="function")throw new Error("invalid callback: "+t);for(;++o<a;)if(r=(e=i[o]).type)n[r]=fi(n[r],e.name,t);else if(t==null)for(r in n)n[r]=fi(n[r],e.name,null);return this},copy:function(){var e={},t=this._;for(var n in t)e[n]=t[n].slice();return new Xe(e)},call:function(e,t){if((r=arguments.length-2)>0)for(var n=new Array(r),i=0,r,o;i<r;++i)n[i]=arguments[i+2];if(!this._.hasOwnProperty(e))throw new Error("unknown type: "+e);for(o=this._[e],i=0,r=o.length;i<r;++i)o[i].value.apply(t,n)},apply:function(e,t,n){if(!this._.hasOwnProperty(e))throw new Error("unknown type: "+e);for(var i=this._[e],r=0,o=i.length;r<o;++r)i[r].value.apply(t,n)}};function Lr(e,t){for(var n=0,i=e.length,r;n<i;++n)if((r=e[n]).name===t)return r.value}function fi(e,t,n){for(var i=0,r=e.length;i<r;++i)if(e[i].name===t){e[i]=Or,e=e.slice(0,i).concat(e.slice(i+1));break}return n!=null&&e.push({name:t,value:n}),e}var ce=0,Se=0,ve=0,di=1e3,qe,Ae,Ze=0,te=0,Qe=0,Ee=typeof performance=="object"&&performance.now?performance:Date,pi=typeof window=="object"&&window.requestAnimationFrame?window.requestAnimationFrame.bind(window):function(e){setTimeout(e,17)};function gi(){return te||(pi(kr),te=Ee.now()+Qe)}function kr(){te=0}function Mn(){this._call=this._time=this._next=null}Mn.prototype=mi.prototype={constructor:Mn,restart:function(e,t,n){if(typeof e!="function")throw new TypeError("callback is not a function");n=(n==null?gi():+n)+(t==null?0:+t),!this._next&&Ae!==this&&(Ae?Ae._next=this:qe=this,Ae=this),this._call=e,this._time=n,Ln()},stop:function(){this._call&&(this._call=null,this._time=1/0,Ln())}};function mi(e,t,n){var i=new Mn;return i.restart(e,t,n),i}function Fr(){gi(),++ce;for(var e=qe,t;e;)(t=te-e._time)>=0&&e._call.call(void 0,t),e=e._next;--ce}function yi(){te=(Ze=Ee.now())+Qe,ce=Se=0;try{Fr()}finally{ce=0,zr(),te=0}}function Pr(){var e=Ee.now(),t=e-Ze;t>di&&(Qe-=t,Ze=e)}function zr(){for(var e,t=qe,n,i=1/0;t;)t._call?(i>t._time&&(i=t._time),e=t,t=t._next):(n=t._next,t._next=null,t=e?e._next=n:qe=n);Ae=e,Ln(i)}function Ln(e){if(!ce){Se&&(Se=clearTimeout(Se));var t=e-te;t>24?(e<1/0&&(Se=setTimeout(yi,e-Ee.now()-Qe)),ve&&(ve=clearInterval(ve))):(ve||(Ze=Ee.now(),ve=setInterval(Pr,di)),ce=1,pi(yi))}}const Br=1664525,Ur=1013904223,_i=4294967296;function Gr(){let e=1;return()=>(e=(Br*e+Ur)%_i)/_i}function Hr(e){return e.x}function jr(e){return e.y}var Wr=10,Vr=Math.PI*(3-Math.sqrt(5));function $r(e){var t,n=1,i=.001,r=1-Math.pow(i,1/300),o=0,a=.6,l=new Map,h=mi(b),s=On("tick","end"),f=Gr();e==null&&(e=[]);function b(){m(),s.call("tick",t),n<i&&(h.stop(),s.call("end",t))}function m(y){var _,T=e.length,v;y===void 0&&(y=1);for(var E=0;E<y;++E)for(n+=(o-n)*r,l.forEach(function(g){g(n)}),_=0;_<T;++_)v=e[_],v.fx==null?v.x+=v.vx*=a:(v.x=v.fx,v.vx=0),v.fy==null?v.y+=v.vy*=a:(v.y=v.fy,v.vy=0);return t}function p(){for(var y=0,_=e.length,T;y<_;++y){if(T=e[y],T.index=y,T.fx!=null&&(T.x=T.fx),T.fy!=null&&(T.y=T.fy),isNaN(T.x)||isNaN(T.y)){var v=Wr*Math.sqrt(.5+y),E=y*Vr;T.x=v*Math.cos(E),T.y=v*Math.sin(E)}(isNaN(T.vx)||isNaN(T.vy))&&(T.vx=T.vy=0)}}function A(y){return y.initialize&&y.initialize(e,f),y}return p(),t={tick:m,restart:function(){return h.restart(b),t},stop:function(){return h.stop(),t},nodes:function(y){return arguments.length?(e=y,p(),l.forEach(A),t):e},alpha:function(y){return arguments.length?(n=+y,t):n},alphaMin:function(y){return arguments.length?(i=+y,t):i},alphaDecay:function(y){return arguments.length?(r=+y,t):+r},alphaTarget:function(y){return arguments.length?(o=+y,t):o},velocityDecay:function(y){return arguments.length?(a=1-y,t):1-a},randomSource:function(y){return arguments.length?(f=y,l.forEach(A),t):f},force:function(y,_){return arguments.length>1?(_==null?l.delete(y):l.set(y,A(_)),t):l.get(y)},find:function(y,_,T){var v=0,E=e.length,g,R,C,k,F;for(T==null?T=1/0:T*=T,v=0;v<E;++v)k=e[v],g=y-k.x,R=_-k.y,C=g*g+R*R,C<T&&(F=k,T=C);return F},on:function(y,_){return arguments.length>1?(s.on(y,_),t):s.on(y)}}}function Yr(){var e,t,n,i,r=nt(-30),o,a=1,l=1/0,h=.81;function s(p){var A,y=e.length,_=Rn(e,Hr,jr).visitAfter(b);for(i=p,A=0;A<y;++A)t=e[A],_.visit(m)}function f(){if(e){var p,A=e.length,y;for(o=new Array(A),p=0;p<A;++p)y=e[p],o[y.index]=+r(y,p,e)}}function b(p){var A=0,y,_,T=0,v,E,g;if(p.length){for(v=E=g=0;g<4;++g)(y=p[g])&&(_=Math.abs(y.value))&&(A+=y.value,T+=_,v+=_*y.x,E+=_*y.y);p.x=v/T,p.y=E/T}else{y=p,y.x=y.data.x,y.y=y.data.y;do A+=o[y.data.index];while(y=y.next)}p.value=A}function m(p,A,y,_){if(!p.value)return!0;var T=p.x-t.x,v=p.y-t.y,E=_-A,g=T*T+v*v;if(E*E/h<g)return g<l&&(T===0&&(T=Wt(n),g+=T*T),v===0&&(v=Wt(n),g+=v*v),g<a&&(g=Math.sqrt(a*g)),t.vx+=T*p.value*i/g,t.vy+=v*p.value*i/g),!0;if(p.length||g>=l)return;(p.data!==t||p.next)&&(T===0&&(T=Wt(n),g+=T*T),v===0&&(v=Wt(n),g+=v*v),g<a&&(g=Math.sqrt(a*g)));do p.data!==t&&(E=o[p.data.index]*i/g,t.vx+=T*E,t.vy+=v*E);while(p=p.next)}return s.initialize=function(p,A){e=p,n=A,f()},s.strength=function(p){return arguments.length?(r=typeof p=="function"?p:nt(+p),f(),s):r},s.distanceMin=function(p){return arguments.length?(a=p*p,s):Math.sqrt(a)},s.distanceMax=function(p){return arguments.length?(l=p*p,s):Math.sqrt(l)},s.theta=function(p){return arguments.length?(h=p*p,s):Math.sqrt(h)},s}function Ti(e,t,n){var i,r=nt(.1),o,a;typeof e!="function"&&(e=nt(+e)),t==null&&(t=0),n==null&&(n=0);function l(s){for(var f=0,b=i.length;f<b;++f){var m=i[f],p=m.x-t||1e-6,A=m.y-n||1e-6,y=Math.sqrt(p*p+A*A),_=(a[f]-y)*o[f]*s/y;m.vx+=p*_,m.vy+=A*_}}function h(){if(i){var s,f=i.length;for(o=new Array(f),a=new Array(f),s=0;s<f;++s)a[s]=+e(i[s],s,i),o[s]=isNaN(a[s])?0:+r(i[s],s,i)}}return l.initialize=function(s){i=s,h()},l.strength=function(s){return arguments.length?(r=typeof s=="function"?s:nt(+s),h(),l):r},l.radius=function(s){return arguments.length?(e=typeof s=="function"?s:nt(+s),h(),l):e},l.x=function(s){return arguments.length?(t=+s,l):t},l.y=function(s){return arguments.length?(n=+s,l):n},l}function wi(e){var t=nt(.1),n,i,r;typeof e!="function"&&(e=nt(e==null?0:+e));function o(l){for(var h=0,s=n.length,f;h<s;++h)f=n[h],f.vx+=(r[h]-f.x)*i[h]*l}function a(){if(n){var l,h=n.length;for(i=new Array(h),r=new Array(h),l=0;l<h;++l)i[l]=isNaN(r[l]=+e(n[l],l,n))?0:+t(n[l],l,n)}}return o.initialize=function(l){n=l,a()},o.strength=function(l){return arguments.length?(t=typeof l=="function"?l:nt(+l),a(),o):t},o.x=function(l){return arguments.length?(e=typeof l=="function"?l:nt(+l),a(),o):e},o}function bi(e){var t=nt(.1),n,i,r;typeof e!="function"&&(e=nt(e==null?0:+e));function o(l){for(var h=0,s=n.length,f;h<s;++h)f=n[h],f.vy+=(r[h]-f.y)*i[h]*l}function a(){if(n){var l,h=n.length;for(i=new Array(h),r=new Array(h),l=0;l<h;++l)i[l]=isNaN(r[l]=+e(n[l],l,n))?0:+t(n[l],l,n)}}return o.initialize=function(l){n=l,a()},o.strength=function(l){return arguments.length?(t=typeof l=="function"?l:nt(+l),a(),o):t},o.y=function(l){return arguments.length?(e=typeof l=="function"?l:nt(+l),a(),o):e},o}function Kr(e=0,t=0,n=.001){let i=[],r;function o(){r=typeof n=="function"?n:()=>n}function a(l){for(let h=0,s=i.length;h<s;++h){const f=i[h],b=r(f,h,i);f.vx&&f.x&&(f.vx-=(f.x-e)*b*l),f.vy&&f.y&&(f.vy-=(f.y-t)*b*l)}}return a.initialize=l=>{i=l,o()},a.x=function(l){return arguments.length?(e=l,a):e},a.y=function(l){return arguments.length?(t=l,a):t},a.strength=function(l){return arguments.length?(n=l,o(),a):n},a}var kn="http://www.w3.org/1999/xhtml",Si={svg:"http://www.w3.org/2000/svg",xhtml:kn,xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/"};function vi(e){var t=e+="",n=t.indexOf(":");return n>=0&&(t=e.slice(0,n))!=="xmlns"&&(e=e.slice(n+1)),Si.hasOwnProperty(t)?{space:Si[t],local:e}:e}function Xr(e){return function(){var t=this.ownerDocument,n=this.namespaceURI;return n===kn&&t.documentElement.namespaceURI===kn?t.createElement(e):t.createElementNS(n,e)}}function qr(e){return function(){return this.ownerDocument.createElementNS(e.space,e.local)}}function Ai(e){var t=vi(e);return(t.local?qr:Xr)(t)}function Zr(){}function Ei(e){return e==null?Zr:function(){return this.querySelector(e)}}function Qr(e){typeof e!="function"&&(e=Ei(e));for(var t=this._groups,n=t.length,i=new Array(n),r=0;r<n;++r)for(var o=t[r],a=o.length,l=i[r]=new Array(a),h,s,f=0;f<a;++f)(h=o[f])&&(s=e.call(h,h.__data__,f,o))&&("__data__"in h&&(s.__data__=h.__data__),l[f]=s);return new wt(i,this._parents)}function Jr(e){return e==null?[]:Array.isArray(e)?e:Array.from(e)}function to(){return[]}function eo(e){return e==null?to:function(){return this.querySelectorAll(e)}}function no(e){return function(){return Jr(e.apply(this,arguments))}}function io(e){typeof e=="function"?e=no(e):e=eo(e);for(var t=this._groups,n=t.length,i=[],r=[],o=0;o<n;++o)for(var a=t[o],l=a.length,h,s=0;s<l;++s)(h=a[s])&&(i.push(e.call(h,h.__data__,s,a)),r.push(h));return new wt(i,r)}function ro(e){return function(){return this.matches(e)}}function xi(e){return function(t){return t.matches(e)}}var oo=Array.prototype.find;function so(e){return function(){return oo.call(this.children,e)}}function ao(){return this.firstElementChild}function lo(e){return this.select(e==null?ao:so(typeof e=="function"?e:xi(e)))}var co=Array.prototype.filter;function uo(){return Array.from(this.children)}function ho(e){return function(){return co.call(this.children,e)}}function fo(e){return this.selectAll(e==null?uo:ho(typeof e=="function"?e:xi(e)))}function po(e){typeof e!="function"&&(e=ro(e));for(var t=this._groups,n=t.length,i=new Array(n),r=0;r<n;++r)for(var o=t[r],a=o.length,l=i[r]=[],h,s=0;s<a;++s)(h=o[s])&&e.call(h,h.__data__,s,o)&&l.push(h);return new wt(i,this._parents)}function Ni(e){return new Array(e.length)}function go(){return new wt(this._enter||this._groups.map(Ni),this._parents)}function Je(e,t){this.ownerDocument=e.ownerDocument,this.namespaceURI=e.namespaceURI,this._next=null,this._parent=e,this.__data__=t}Je.prototype={constructor:Je,appendChild:function(e){return this._parent.insertBefore(e,this._next)},insertBefore:function(e,t){return this._parent.insertBefore(e,t)},querySelector:function(e){return this._parent.querySelector(e)},querySelectorAll:function(e){return this._parent.querySelectorAll(e)}};function mo(e){return function(){return e}}function yo(e,t,n,i,r,o){for(var a=0,l,h=t.length,s=o.length;a<s;++a)(l=t[a])?(l.__data__=o[a],i[a]=l):n[a]=new Je(e,o[a]);for(;a<h;++a)(l=t[a])&&(r[a]=l)}function _o(e,t,n,i,r,o,a){var l,h,s=new Map,f=t.length,b=o.length,m=new Array(f),p;for(l=0;l<f;++l)(h=t[l])&&(m[l]=p=a.call(h,h.__data__,l,t)+"",s.has(p)?r[l]=h:s.set(p,h));for(l=0;l<b;++l)p=a.call(e,o[l],l,o)+"",(h=s.get(p))?(i[l]=h,h.__data__=o[l],s.delete(p)):n[l]=new Je(e,o[l]);for(l=0;l<f;++l)(h=t[l])&&s.get(m[l])===h&&(r[l]=h)}function To(e){return e.__data__}function wo(e,t){if(!arguments.length)return Array.from(this,To);var n=t?_o:yo,i=this._parents,r=this._groups;typeof e!="function"&&(e=mo(e));for(var o=r.length,a=new Array(o),l=new Array(o),h=new Array(o),s=0;s<o;++s){var f=i[s],b=r[s],m=b.length,p=bo(e.call(f,f&&f.__data__,s,i)),A=p.length,y=l[s]=new Array(A),_=a[s]=new Array(A),T=h[s]=new Array(m);n(f,b,y,_,T,p,t);for(var v=0,E=0,g,R;v<A;++v)if(g=y[v]){for(v>=E&&(E=v+1);!(R=_[E])&&++E<A;);g._next=R||null}}return a=new wt(a,i),a._enter=l,a._exit=h,a}function bo(e){return typeof e=="object"&&"length"in e?e:Array.from(e)}function So(){return new wt(this._exit||this._groups.map(Ni),this._parents)}function vo(e,t,n){var i=this.enter(),r=this,o=this.exit();return typeof e=="function"?(i=e(i),i&&(i=i.selection())):i=i.append(e+""),t!=null&&(r=t(r),r&&(r=r.selection())),n==null?o.remove():n(o),i&&r?i.merge(r).order():r}function Ao(e){for(var t=e.selection?e.selection():e,n=this._groups,i=t._groups,r=n.length,o=i.length,a=Math.min(r,o),l=new Array(r),h=0;h<a;++h)for(var s=n[h],f=i[h],b=s.length,m=l[h]=new Array(b),p,A=0;A<b;++A)(p=s[A]||f[A])&&(m[A]=p);for(;h<r;++h)l[h]=n[h];return new wt(l,this._parents)}function Eo(){for(var e=this._groups,t=-1,n=e.length;++t<n;)for(var i=e[t],r=i.length-1,o=i[r],a;--r>=0;)(a=i[r])&&(o&&a.compareDocumentPosition(o)^4&&o.parentNode.insertBefore(a,o),o=a);return this}function xo(e){e||(e=No);function t(b,m){return b&&m?e(b.__data__,m.__data__):!b-!m}for(var n=this._groups,i=n.length,r=new Array(i),o=0;o<i;++o){for(var a=n[o],l=a.length,h=r[o]=new Array(l),s,f=0;f<l;++f)(s=a[f])&&(h[f]=s);h.sort(t)}return new wt(r,this._parents).order()}function No(e,t){return e<t?-1:e>t?1:e>=t?0:NaN}function Do(){var e=arguments[0];return arguments[0]=this,e.apply(null,arguments),this}function Io(){return Array.from(this)}function Ro(){for(var e=this._groups,t=0,n=e.length;t<n;++t)for(var i=e[t],r=0,o=i.length;r<o;++r){var a=i[r];if(a)return a}return null}function Co(){let e=0;for(const t of this)++e;return e}function Oo(){return!this.node()}function Mo(e){for(var t=this._groups,n=0,i=t.length;n<i;++n)for(var r=t[n],o=0,a=r.length,l;o<a;++o)(l=r[o])&&e.call(l,l.__data__,o,r);return this}function Lo(e){return function(){this.removeAttribute(e)}}function ko(e){return function(){this.removeAttributeNS(e.space,e.local)}}function Fo(e,t){return function(){this.setAttribute(e,t)}}function Po(e,t){return function(){this.setAttributeNS(e.space,e.local,t)}}function zo(e,t){return function(){var n=t.apply(this,arguments);n==null?this.removeAttribute(e):this.setAttribute(e,n)}}function Bo(e,t){return function(){var n=t.apply(this,arguments);n==null?this.removeAttributeNS(e.space,e.local):this.setAttributeNS(e.space,e.local,n)}}function Uo(e,t){var n=vi(e);if(arguments.length<2){var i=this.node();return n.local?i.getAttributeNS(n.space,n.local):i.getAttribute(n)}return this.each((t==null?n.local?ko:Lo:typeof t=="function"?n.local?Bo:zo:n.local?Po:Fo)(n,t))}function Di(e){return e.ownerDocument&&e.ownerDocument.defaultView||e.document&&e||e.defaultView}function Go(e){return function(){this.style.removeProperty(e)}}function Ho(e,t,n){return function(){this.style.setProperty(e,t,n)}}function jo(e,t,n){return function(){var i=t.apply(this,arguments);i==null?this.style.removeProperty(e):this.style.setProperty(e,i,n)}}function Wo(e,t,n){return arguments.length>1?this.each((t==null?Go:typeof t=="function"?jo:Ho)(e,t,n??"")):Vo(this.node(),e)}function Vo(e,t){return e.style.getPropertyValue(t)||Di(e).getComputedStyle(e,null).getPropertyValue(t)}function $o(e){return function(){delete this[e]}}function Yo(e,t){return function(){this[e]=t}}function Ko(e,t){return function(){var n=t.apply(this,arguments);n==null?delete this[e]:this[e]=n}}function Xo(e,t){return arguments.length>1?this.each((t==null?$o:typeof t=="function"?Ko:Yo)(e,t)):this.node()[e]}function Ii(e){return e.trim().split(/^|\\s+/)}function Fn(e){return e.classList||new Ri(e)}function Ri(e){this._node=e,this._names=Ii(e.getAttribute("class")||"")}Ri.prototype={add:function(e){var t=this._names.indexOf(e);t<0&&(this._names.push(e),this._node.setAttribute("class",this._names.join(" ")))},remove:function(e){var t=this._names.indexOf(e);t>=0&&(this._names.splice(t,1),this._node.setAttribute("class",this._names.join(" ")))},contains:function(e){return this._names.indexOf(e)>=0}};function Ci(e,t){for(var n=Fn(e),i=-1,r=t.length;++i<r;)n.add(t[i])}function Oi(e,t){for(var n=Fn(e),i=-1,r=t.length;++i<r;)n.remove(t[i])}function qo(e){return function(){Ci(this,e)}}function Zo(e){return function(){Oi(this,e)}}function Qo(e,t){return function(){(t.apply(this,arguments)?Ci:Oi)(this,e)}}function Jo(e,t){var n=Ii(e+"");if(arguments.length<2){for(var i=Fn(this.node()),r=-1,o=n.length;++r<o;)if(!i.contains(n[r]))return!1;return!0}return this.each((typeof t=="function"?Qo:t?qo:Zo)(n,t))}function ts(){this.textContent=""}function es(e){return function(){this.textContent=e}}function ns(e){return function(){var t=e.apply(this,arguments);this.textContent=t??""}}function is(e){return arguments.length?this.each(e==null?ts:(typeof e=="function"?ns:es)(e)):this.node().textContent}function rs(){this.innerHTML=""}function os(e){return function(){this.innerHTML=e}}function ss(e){return function(){var t=e.apply(this,arguments);this.innerHTML=t??""}}function as(e){return arguments.length?this.each(e==null?rs:(typeof e=="function"?ss:os)(e)):this.node().innerHTML}function ls(){this.nextSibling&&this.parentNode.appendChild(this)}function cs(){return this.each(ls)}function us(){this.previousSibling&&this.parentNode.insertBefore(this,this.parentNode.firstChild)}function hs(){return this.each(us)}function fs(e){var t=typeof e=="function"?e:Ai(e);return this.select(function(){return this.appendChild(t.apply(this,arguments))})}function ds(){return null}function ps(e,t){var n=typeof e=="function"?e:Ai(e),i=t==null?ds:typeof t=="function"?t:Ei(t);return this.select(function(){return this.insertBefore(n.apply(this,arguments),i.apply(this,arguments)||null)})}function gs(){var e=this.parentNode;e&&e.removeChild(this)}function ms(){return this.each(gs)}function ys(){var e=this.cloneNode(!1),t=this.parentNode;return t?t.insertBefore(e,this.nextSibling):e}function _s(){var e=this.cloneNode(!0),t=this.parentNode;return t?t.insertBefore(e,this.nextSibling):e}function Ts(e){return this.select(e?_s:ys)}function ws(e){return arguments.length?this.property("__data__",e):this.node().__data__}function bs(e){return function(t){e.call(this,t,this.__data__)}}function Ss(e){return e.trim().split(/^|\\s+/).map(function(t){var n="",i=t.indexOf(".");return i>=0&&(n=t.slice(i+1),t=t.slice(0,i)),{type:t,name:n}})}function vs(e){return function(){var t=this.__on;if(t){for(var n=0,i=-1,r=t.length,o;n<r;++n)o=t[n],(!e.type||o.type===e.type)&&o.name===e.name?this.removeEventListener(o.type,o.listener,o.options):t[++i]=o;++i?t.length=i:delete this.__on}}}function As(e,t,n){return function(){var i=this.__on,r,o=bs(t);if(i){for(var a=0,l=i.length;a<l;++a)if((r=i[a]).type===e.type&&r.name===e.name){this.removeEventListener(r.type,r.listener,r.options),this.addEventListener(r.type,r.listener=o,r.options=n),r.value=t;return}}this.addEventListener(e.type,o,n),r={type:e.type,name:e.name,value:t,listener:o,options:n},i?i.push(r):this.__on=[r]}}function Es(e,t,n){var i=Ss(e+""),r,o=i.length,a;if(arguments.length<2){var l=this.node().__on;if(l){for(var h=0,s=l.length,f;h<s;++h)for(r=0,f=l[h];r<o;++r)if((a=i[r]).type===f.type&&a.name===f.name)return f.value}return}for(l=t?As:vs,r=0;r<o;++r)this.each(l(i[r],t,n));return this}function Mi(e,t,n){var i=Di(e),r=i.CustomEvent;typeof r=="function"?r=new r(t,n):(r=i.document.createEvent("Event"),n?(r.initEvent(t,n.bubbles,n.cancelable),r.detail=n.detail):r.initEvent(t,!1,!1)),e.dispatchEvent(r)}function xs(e,t){return function(){return Mi(this,e,t)}}function Ns(e,t){return function(){return Mi(this,e,t.apply(this,arguments))}}function Ds(e,t){return this.each((typeof t=="function"?Ns:xs)(e,t))}function*Is(){for(var e=this._groups,t=0,n=e.length;t<n;++t)for(var i=e[t],r=0,o=i.length,a;r<o;++r)(a=i[r])&&(yield a)}var Rs=[null];function wt(e,t){this._groups=e,this._parents=t}function Cs(){return this}wt.prototype={constructor:wt,select:Qr,selectAll:io,selectChild:lo,selectChildren:fo,filter:po,data:wo,enter:go,exit:So,join:vo,merge:Ao,selection:Cs,order:Eo,sort:xo,call:Do,nodes:Io,node:Ro,size:Co,empty:Oo,each:Mo,attr:Uo,style:Wo,property:Xo,classed:Jo,text:is,html:as,raise:cs,lower:hs,append:fs,insert:ps,remove:ms,clone:Ts,datum:ws,on:Es,dispatch:Ds,[Symbol.iterator]:Is};function tn(e){return typeof e=="string"?new wt([[document.querySelector(e)]],[document.documentElement]):new wt([[e]],Rs)}function Os(e){let t;for(;t=e.sourceEvent;)e=t;return e}function Li(e,t){if(e=Os(e),t===void 0&&(t=e.currentTarget),t){var n=t.ownerSVGElement||t;if(n.createSVGPoint){var i=n.createSVGPoint();return i.x=e.clientX,i.y=e.clientY,i=i.matrixTransform(t.getScreenCTM().inverse()),[i.x,i.y]}if(t.getBoundingClientRect){var r=t.getBoundingClientRect();return[e.clientX-r.left-t.clientLeft,e.clientY-r.top-t.clientTop]}}return[e.pageX,e.pageY]}const Ms={passive:!1},xe={capture:!0,passive:!1};function Pn(e){e.stopImmediatePropagation()}function ue(e){e.preventDefault(),e.stopImmediatePropagation()}function Ls(e){var t=e.document.documentElement,n=tn(e).on("dragstart.drag",ue,xe);"onselectstart"in t?n.on("selectstart.drag",ue,xe):(t.__noselect=t.style.MozUserSelect,t.style.MozUserSelect="none")}function ks(e,t){var n=e.document.documentElement,i=tn(e).on("dragstart.drag",null);t&&(i.on("click.drag",ue,xe),setTimeout(function(){i.on("click.drag",null)},0)),"onselectstart"in n?i.on("selectstart.drag",null):(n.style.MozUserSelect=n.__noselect,delete n.__noselect)}var en=e=>()=>e;function zn(e,{sourceEvent:t,subject:n,target:i,identifier:r,active:o,x:a,y:l,dx:h,dy:s,dispatch:f}){Object.defineProperties(this,{type:{value:e,enumerable:!0,configurable:!0},sourceEvent:{value:t,enumerable:!0,configurable:!0},subject:{value:n,enumerable:!0,configurable:!0},target:{value:i,enumerable:!0,configurable:!0},identifier:{value:r,enumerable:!0,configurable:!0},active:{value:o,enumerable:!0,configurable:!0},x:{value:a,enumerable:!0,configurable:!0},y:{value:l,enumerable:!0,configurable:!0},dx:{value:h,enumerable:!0,configurable:!0},dy:{value:s,enumerable:!0,configurable:!0},_:{value:f}})}zn.prototype.on=function(){var e=this._.on.apply(this._,arguments);return e===this._?this:e};function Fs(e){return!e.ctrlKey&&!e.button}function Ps(){return this.parentNode}function zs(e,t){return t??{x:e.x,y:e.y}}function Bs(){return navigator.maxTouchPoints||"ontouchstart"in this}function Us(){var e=Fs,t=Ps,n=zs,i=Bs,r={},o=On("start","drag","end"),a=0,l,h,s,f,b=0;function m(g){g.on("mousedown.drag",p).filter(i).on("touchstart.drag",_).on("touchmove.drag",T,Ms).on("touchend.drag touchcancel.drag",v).style("touch-action","none").style("-webkit-tap-highlight-color","rgba(0,0,0,0)")}function p(g,R){if(!(f||!e.call(this,g,R))){var C=E(this,t.call(this,g,R),g,R,"mouse");C&&(tn(g.view).on("mousemove.drag",A,xe).on("mouseup.drag",y,xe),Ls(g.view),Pn(g),s=!1,l=g.clientX,h=g.clientY,C("start",g))}}function A(g){if(ue(g),!s){var R=g.clientX-l,C=g.clientY-h;s=R*R+C*C>b}r.mouse("drag",g)}function y(g){tn(g.view).on("mousemove.drag mouseup.drag",null),ks(g.view,s),ue(g),r.mouse("end",g)}function _(g,R){if(e.call(this,g,R)){var C=g.changedTouches,k=t.call(this,g,R),F=C.length,$,G;for($=0;$<F;++$)(G=E(this,k,g,R,C[$].identifier,C[$]))&&(Pn(g),G("start",g,C[$]))}}function T(g){var R=g.changedTouches,C=R.length,k,F;for(k=0;k<C;++k)(F=r[R[k].identifier])&&(ue(g),F("drag",g,R[k]))}function v(g){var R=g.changedTouches,C=R.length,k,F;for(f&&clearTimeout(f),f=setTimeout(function(){f=null},500),k=0;k<C;++k)(F=r[R[k].identifier])&&(Pn(g),F("end",g,R[k]))}function E(g,R,C,k,F,$){var G=o.copy(),Q=Li($||C,R),ht,st,K;if((K=n.call(g,new zn("beforestart",{sourceEvent:C,target:m,identifier:F,active:a,x:Q[0],y:Q[1],dx:0,dy:0,dispatch:G}),k))!=null)return ht=K.x-Q[0]||0,st=K.y-Q[1]||0,function Vt(Rt,kt,Me){var ge=Q,ne;switch(Rt){case"start":r[F]=Vt,ne=a++;break;case"end":delete r[F],--a;case"drag":Q=Li(Me||kt,R),ne=a;break}G.call(Rt,g,new zn(Rt,{sourceEvent:kt,subject:K,target:m,identifier:F,active:ne,x:Q[0]+ht,y:Q[1]+st,dx:Q[0]-ge[0],dy:Q[1]-ge[1],dispatch:G}),k)}}return m.filter=function(g){return arguments.length?(e=typeof g=="function"?g:en(!!g),m):e},m.container=function(g){return arguments.length?(t=typeof g=="function"?g:en(g),m):t},m.subject=function(g){return arguments.length?(n=typeof g=="function"?g:en(g),m):n},m.touchable=function(g){return arguments.length?(i=typeof g=="function"?g:en(!!g),m):i},m.on=function(){var g=o.on.apply(o,arguments);return g===o?m:g},m.clickDistance=function(g){return arguments.length?(b=(g=+g)*g,m):Math.sqrt(b)},m}/*! @license DOMPurify 3.4.7 | (c) Cure53 and other contributors | Released under the Apache license 2.0 and Mozilla Public License 2.0 | github.com/cure53/DOMPurify/blob/3.4.7/LICENSE */function ki(e,t){(t==null||t>e.length)&&(t=e.length);for(var n=0,i=Array(t);n<t;n++)i[n]=e[n];return i}function Gs(e){if(Array.isArray(e))return e}function Hs(e,t){var n=e==null?null:typeof Symbol<"u"&&e[Symbol.iterator]||e["@@iterator"];if(n!=null){var i,r,o,a,l=[],h=!0,s=!1;try{if(o=(n=n.call(e)).next,t!==0)for(;!(h=(i=o.call(n)).done)&&(l.push(i.value),l.length!==t);h=!0);}catch(f){s=!0,r=f}finally{try{if(!h&&n.return!=null&&(a=n.return(),Object(a)!==a))return}finally{if(s)throw r}}return l}}function js(){throw new TypeError(`Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method.`)}function Ws(e,t){return Gs(e)||Hs(e,t)||Vs(e,t)||js()}function Vs(e,t){if(e){if(typeof e=="string")return ki(e,t);var n={}.toString.call(e).slice(8,-1);return n==="Object"&&e.constructor&&(n=e.constructor.name),n==="Map"||n==="Set"?Array.from(e):n==="Arguments"||/^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)?ki(e,t):void 0}}const Fi=Object.entries,Pi=Object.setPrototypeOf,$s=Object.isFrozen,Ys=Object.getPrototypeOf,Ks=Object.getOwnPropertyDescriptor;let ct=Object.freeze,bt=Object.seal,he=Object.create,zi=typeof Reflect<"u"&&Reflect,Bn=zi.apply,Un=zi.construct;ct||(ct=function(t){return t}),bt||(bt=function(t){return t}),Bn||(Bn=function(t,n){for(var i=arguments.length,r=new Array(i>2?i-2:0),o=2;o<i;o++)r[o-2]=arguments[o];return t.apply(n,r)}),Un||(Un=function(t){for(var n=arguments.length,i=new Array(n>1?n-1:0),r=1;r<n;r++)i[r-1]=arguments[r];return new t(...i)});const fe=tt(Array.prototype.forEach),Xs=tt(Array.prototype.lastIndexOf),Bi=tt(Array.prototype.pop),de=tt(Array.prototype.push),qs=tt(Array.prototype.splice),ut=Array.isArray,Ne=tt(String.prototype.toLowerCase),Gn=tt(String.prototype.toString),Ui=tt(String.prototype.match),pe=tt(String.prototype.replace),Gi=tt(String.prototype.indexOf),Zs=tt(String.prototype.trim),Qs=tt(Number.prototype.toString),Js=tt(Boolean.prototype.toString),Hi=typeof BigInt>"u"?null:tt(BigInt.prototype.toString),ji=typeof Symbol>"u"?null:tt(Symbol.prototype.toString),J=tt(Object.prototype.hasOwnProperty),De=tt(Object.prototype.toString),rt=tt(RegExp.prototype.test),Ie=ta(TypeError);function tt(e){return function(t){t instanceof RegExp&&(t.lastIndex=0);for(var n=arguments.length,i=new Array(n>1?n-1:0),r=1;r<n;r++)i[r-1]=arguments[r];return Bn(e,t,i)}}function ta(e){return function(){for(var t=arguments.length,n=new Array(t),i=0;i<t;i++)n[i]=arguments[i];return Un(e,n)}}function M(e,t){let n=arguments.length>2&&arguments[2]!==void 0?arguments[2]:Ne;if(Pi&&Pi(e,null),!ut(t))return e;let i=t.length;for(;i--;){let r=t[i];if(typeof r=="string"){const o=n(r);o!==r&&($s(t)||(t[i]=o),r=o)}e[r]=!0}return e}function ea(e){for(let t=0;t<e.length;t++)J(e,t)||(e[t]=null);return e}function ot(e){const t=he(null);for(const i of Fi(e)){var n=Ws(i,2);const r=n[0],o=n[1];J(e,r)&&(ut(o)?t[r]=ea(o):o&&typeof o=="object"&&o.constructor===Object?t[r]=ot(o):t[r]=o)}return t}function na(e){switch(typeof e){case"string":return e;case"number":return Qs(e);case"boolean":return Js(e);case"bigint":return Hi?Hi(e):"0";case"symbol":return ji?ji(e):"Symbol()";case"undefined":return De(e);case"function":case"object":{if(e===null)return De(e);const t=e,n=Dt(t,"toString");if(typeof n=="function"){const i=n(t);return typeof i=="string"?i:De(i)}return De(e)}default:return De(e)}}function Dt(e,t){for(;e!==null;){const i=Ks(e,t);if(i){if(i.get)return tt(i.get);if(typeof i.value=="function")return tt(i.value)}e=Ys(e)}function n(){return null}return n}function ia(e){try{return rt(e,""),!0}catch{return!1}}const Wi=ct(["a","abbr","acronym","address","area","article","aside","audio","b","bdi","bdo","big","blink","blockquote","body","br","button","canvas","caption","center","cite","code","col","colgroup","content","data","datalist","dd","decorator","del","details","dfn","dialog","dir","div","dl","dt","element","em","fieldset","figcaption","figure","font","footer","form","h1","h2","h3","h4","h5","h6","head","header","hgroup","hr","html","i","img","input","ins","kbd","label","legend","li","main","map","mark","marquee","menu","menuitem","meter","nav","nobr","ol","optgroup","option","output","p","picture","pre","progress","q","rp","rt","ruby","s","samp","search","section","select","shadow","slot","small","source","spacer","span","strike","strong","style","sub","summary","sup","table","tbody","td","template","textarea","tfoot","th","thead","time","tr","track","tt","u","ul","var","video","wbr"]),Hn=ct(["svg","a","altglyph","altglyphdef","altglyphitem","animatecolor","animatemotion","animatetransform","circle","clippath","defs","desc","ellipse","enterkeyhint","exportparts","filter","font","g","glyph","glyphref","hkern","image","inputmode","line","lineargradient","marker","mask","metadata","mpath","part","path","pattern","polygon","polyline","radialgradient","rect","stop","style","switch","symbol","text","textpath","title","tref","tspan","view","vkern"]),jn=ct(["feBlend","feColorMatrix","feComponentTransfer","feComposite","feConvolveMatrix","feDiffuseLighting","feDisplacementMap","feDistantLight","feDropShadow","feFlood","feFuncA","feFuncB","feFuncG","feFuncR","feGaussianBlur","feImage","feMerge","feMergeNode","feMorphology","feOffset","fePointLight","feSpecularLighting","feSpotLight","feTile","feTurbulence"]),ra=ct(["animate","color-profile","cursor","discard","font-face","font-face-format","font-face-name","font-face-src","font-face-uri","foreignobject","hatch","hatchpath","mesh","meshgradient","meshpatch","meshrow","missing-glyph","script","set","solidcolor","unknown","use"]),Wn=ct(["math","menclose","merror","mfenced","mfrac","mglyph","mi","mlabeledtr","mmultiscripts","mn","mo","mover","mpadded","mphantom","mroot","mrow","ms","mspace","msqrt","mstyle","msub","msup","msubsup","mtable","mtd","mtext","mtr","munder","munderover","mprescripts"]),oa=ct(["maction","maligngroup","malignmark","mlongdiv","mscarries","mscarry","msgroup","mstack","msline","msrow","semantics","annotation","annotation-xml","mprescripts","none"]),Vi=ct(["#text"]),$i=ct(["accept","action","align","alt","autocapitalize","autocomplete","autopictureinpicture","autoplay","background","bgcolor","border","capture","cellpadding","cellspacing","checked","cite","class","clear","color","cols","colspan","command","commandfor","controls","controlslist","coords","crossorigin","datetime","decoding","default","dir","disabled","disablepictureinpicture","disableremoteplayback","download","draggable","enctype","enterkeyhint","exportparts","face","for","headers","height","hidden","high","href","hreflang","id","inert","inputmode","integrity","ismap","kind","label","lang","list","loading","loop","low","max","maxlength","media","method","min","minlength","multiple","muted","name","nonce","noshade","novalidate","nowrap","open","optimum","part","pattern","placeholder","playsinline","popover","popovertarget","popovertargetaction","poster","preload","pubdate","radiogroup","readonly","rel","required","rev","reversed","role","rows","rowspan","spellcheck","scope","selected","shape","size","sizes","slot","span","srclang","start","src","srcset","step","style","summary","tabindex","title","translate","type","usemap","valign","value","width","wrap","xmlns"]),Vn=ct(["accent-height","accumulate","additive","alignment-baseline","amplitude","ascent","attributename","attributetype","azimuth","basefrequency","baseline-shift","begin","bias","by","class","clip","clippathunits","clip-path","clip-rule","color","color-interpolation","color-interpolation-filters","color-profile","color-rendering","cx","cy","d","dx","dy","diffuseconstant","direction","display","divisor","dur","edgemode","elevation","end","exponent","fill","fill-opacity","fill-rule","filter","filterunits","flood-color","flood-opacity","font-family","font-size","font-size-adjust","font-stretch","font-style","font-variant","font-weight","fx","fy","g1","g2","glyph-name","glyphref","gradientunits","gradienttransform","height","href","id","image-rendering","in","in2","intercept","k","k1","k2","k3","k4","kerning","keypoints","keysplines","keytimes","lang","lengthadjust","letter-spacing","kernelmatrix","kernelunitlength","lighting-color","local","marker-end","marker-mid","marker-start","markerheight","markerunits","markerwidth","maskcontentunits","maskunits","max","mask","mask-type","media","method","mode","min","name","numoctaves","offset","operator","opacity","order","orient","orientation","origin","overflow","paint-order","path","pathlength","patterncontentunits","patterntransform","patternunits","points","preservealpha","preserveaspectratio","primitiveunits","r","rx","ry","radius","refx","refy","repeatcount","repeatdur","restart","result","rotate","scale","seed","shape-rendering","slope","specularconstant","specularexponent","spreadmethod","startoffset","stddeviation","stitchtiles","stop-color","stop-opacity","stroke-dasharray","stroke-dashoffset","stroke-linecap","stroke-linejoin","stroke-miterlimit","stroke-opacity","stroke","stroke-width","style","surfacescale","systemlanguage","tabindex","tablevalues","targetx","targety","transform","transform-origin","text-anchor","text-decoration","text-rendering","textlength","type","u1","u2","unicode","values","viewbox","visibility","version","vert-adv-y","vert-origin-x","vert-origin-y","width","word-spacing","wrap","writing-mode","xchannelselector","ychannelselector","x","x1","x2","xmlns","y","y1","y2","z","zoomandpan"]),Yi=ct(["accent","accentunder","align","bevelled","close","columnalign","columnlines","columnspacing","columnspan","denomalign","depth","dir","display","displaystyle","encoding","fence","frame","height","href","id","largeop","length","linethickness","lquote","lspace","mathbackground","mathcolor","mathsize","mathvariant","maxsize","minsize","movablelimits","notation","numalign","open","rowalign","rowlines","rowspacing","rowspan","rspace","rquote","scriptlevel","scriptminsize","scriptsizemultiplier","selection","separator","separators","stretchy","subscriptshift","supscriptshift","symmetric","voffset","width","xmlns"]),nn=ct(["xlink:href","xml:id","xlink:title","xml:space","xmlns:xlink"]),sa=bt(/{{[\\w\\W]*|^[\\w\\W]*}}/g),aa=bt(/<%[\\w\\W]*|^[\\w\\W]*%>/g),la=bt(/\\${[\\w\\W]*/g),ca=bt(/^data-[\\-\\w.\\u00B7-\\uFFFF]+$/),ua=bt(/^aria-[\\-\\w]+$/),Ki=bt(/^(?:(?:(?:f|ht)tps?|mailto|tel|callto|sms|cid|xmpp|matrix):|[^a-z]|[a-z+.\\-]+(?:[^a-z+.\\-:]|$))/i),ha=bt(/^(?:\\w+script|data):/i),fa=bt(/[\\u0000-\\u0020\\u00A0\\u1680\\u180E\\u2000-\\u2029\\u205F\\u3000]/g),da=bt(/^html$/i),pa=bt(/^[a-z][.\\w]*(-[.\\w]+)+$/i),It={element:1,attribute:2,text:3,cdataSection:4,entityReference:5,entityNode:6,progressingInstruction:7,comment:8,document:9,documentType:10,documentFragment:11,notation:12},ga=function(){return typeof window>"u"?null:window},ma=function(t,n){if(typeof t!="object"||typeof t.createPolicy!="function")return null;let i=null;const r="data-tt-policy-suffix";n&&n.hasAttribute(r)&&(i=n.getAttribute(r));const o="dompurify"+(i?"#"+i:"");try{return t.createPolicy(o,{createHTML(a){return a},createScriptURL(a){return a}})}catch{return console.warn("TrustedTypes policy "+o+" could not be created."),null}},Xi=function(){return{afterSanitizeAttributes:[],afterSanitizeElements:[],afterSanitizeShadowDOM:[],beforeSanitizeAttributes:[],beforeSanitizeElements:[],beforeSanitizeShadowDOM:[],uponSanitizeAttribute:[],uponSanitizeElement:[],uponSanitizeShadowNode:[]}};function qi(){let e=arguments.length>0&&arguments[0]!==void 0?arguments[0]:ga();const t=D=>qi(D);if(t.version="3.4.7",t.removed=[],!e||!e.document||e.document.nodeType!==It.document||!e.Element)return t.isSupported=!1,t;let n=e.document;const i=n,r=i.currentScript;e.DocumentFragment;const o=e.HTMLTemplateElement,a=e.Node,l=e.Element,h=e.NodeFilter,s=e.NamedNodeMap;s===void 0&&(e.NamedNodeMap||e.MozNamedAttrMap),e.HTMLFormElement;const f=e.DOMParser,b=e.trustedTypes,m=l.prototype,p=Dt(m,"cloneNode"),A=Dt(m,"remove"),y=Dt(m,"nextSibling"),_=Dt(m,"childNodes"),T=Dt(m,"parentNode"),v=Dt(m,"shadowRoot"),E=Dt(m,"attributes"),g=a&&a.prototype?Dt(a.prototype,"nodeType"):null,R=a&&a.prototype?Dt(a.prototype,"nodeName"):null;if(typeof o=="function"){const D=n.createElement("template");D.content&&D.content.ownerDocument&&(n=D.content.ownerDocument)}let C,k="";const F=n,$=F.implementation,G=F.createNodeIterator,Q=F.createDocumentFragment,ht=F.getElementsByTagName,st=i.importNode;let K=Xi();t.isSupported=typeof Fi=="function"&&typeof T=="function"&&$&&$.createHTMLDocument!==void 0;const Vt=sa,Rt=aa,kt=la,Me=ca,ge=ua,ne=ha,pn=fa,Xn=pa;let H=Ki,X=null;const Le=M({},[...Wi,...Hn,...jn,...Wn,...Vi]);let Y=null;const me=M({},[...$i,...Vn,...Yi,...nn]);let j=Object.seal(he(null,{tagNameCheck:{writable:!0,configurable:!1,enumerable:!0,value:null},attributeNameCheck:{writable:!0,configurable:!1,enumerable:!0,value:null},allowCustomizedBuiltInElements:{writable:!0,configurable:!1,enumerable:!0,value:!1}})),$t=null,Yt=null;const vt=Object.seal(he(null,{tagCheck:{writable:!0,configurable:!1,enumerable:!0,value:null},attributeCheck:{writable:!0,configurable:!1,enumerable:!0,value:null}}));let ke=!0,Fe=!0,gn=!1,mn=!0,Ct=!1,ie=!0,Ft=!1,Pe=!1,Kt=!1,Ot=!1,Pt=!1,pt=!1,ze=!0,Be=!1;const yn="user-content-";let Ue=!0,Mt=!1,zt={},gt=null;const ye=M({},["annotation-xml","audio","colgroup","desc","foreignobject","head","iframe","math","mi","mn","mo","ms","mtext","noembed","noframes","noscript","plaintext","script","style","svg","template","thead","title","video","xmp"]);let Ge=null;const _n=M({},["audio","video","img","source","image","track"]);let He=null;const Lt=M({},["alt","class","for","id","label","name","pattern","placeholder","role","summary","title","value","style","xmlns"]),Bt="http://www.w3.org/1998/Math/MathML",_e="http://www.w3.org/2000/svg",mt="http://www.w3.org/1999/xhtml";let Xt=mt,Te=!1,Ut=null;const qn=M({},[Bt,_e,mt],Gn);let At=M({},["mi","mo","mn","ms","mtext"]),je=M({},["annotation-xml"]);const Zn=M({},["title","style","font","a","script"]);let re=null;const Qn=["application/xhtml+xml","text/html"],Jn="text/html";let B=null,qt=null;const ti=n.createElement("form"),Tn=function(u){return u instanceof RegExp||u instanceof Function},We=function(){let u=arguments.length>0&&arguments[0]!==void 0?arguments[0]:{};if(qt&&qt===u)return;(!u||typeof u!="object")&&(u={}),u=ot(u),re=Qn.indexOf(u.PARSER_MEDIA_TYPE)===-1?Jn:u.PARSER_MEDIA_TYPE,B=re==="application/xhtml+xml"?Gn:Ne,X=J(u,"ALLOWED_TAGS")&&ut(u.ALLOWED_TAGS)?M({},u.ALLOWED_TAGS,B):Le,Y=J(u,"ALLOWED_ATTR")&&ut(u.ALLOWED_ATTR)?M({},u.ALLOWED_ATTR,B):me,Ut=J(u,"ALLOWED_NAMESPACES")&&ut(u.ALLOWED_NAMESPACES)?M({},u.ALLOWED_NAMESPACES,Gn):qn,He=J(u,"ADD_URI_SAFE_ATTR")&&ut(u.ADD_URI_SAFE_ATTR)?M(ot(Lt),u.ADD_URI_SAFE_ATTR,B):Lt,Ge=J(u,"ADD_DATA_URI_TAGS")&&ut(u.ADD_DATA_URI_TAGS)?M(ot(_n),u.ADD_DATA_URI_TAGS,B):_n,gt=J(u,"FORBID_CONTENTS")&&ut(u.FORBID_CONTENTS)?M({},u.FORBID_CONTENTS,B):ye,$t=J(u,"FORBID_TAGS")&&ut(u.FORBID_TAGS)?M({},u.FORBID_TAGS,B):ot({}),Yt=J(u,"FORBID_ATTR")&&ut(u.FORBID_ATTR)?M({},u.FORBID_ATTR,B):ot({}),zt=J(u,"USE_PROFILES")?u.USE_PROFILES&&typeof u.USE_PROFILES=="object"?ot(u.USE_PROFILES):u.USE_PROFILES:!1,ke=u.ALLOW_ARIA_ATTR!==!1,Fe=u.ALLOW_DATA_ATTR!==!1,gn=u.ALLOW_UNKNOWN_PROTOCOLS||!1,mn=u.ALLOW_SELF_CLOSE_IN_ATTR!==!1,Ct=u.SAFE_FOR_TEMPLATES||!1,ie=u.SAFE_FOR_XML!==!1,Ft=u.WHOLE_DOCUMENT||!1,Ot=u.RETURN_DOM||!1,Pt=u.RETURN_DOM_FRAGMENT||!1,pt=u.RETURN_TRUSTED_TYPE||!1,Kt=u.FORCE_BODY||!1,ze=u.SANITIZE_DOM!==!1,Be=u.SANITIZE_NAMED_PROPS||!1,Ue=u.KEEP_CONTENT!==!1,Mt=u.IN_PLACE||!1,H=ia(u.ALLOWED_URI_REGEXP)?u.ALLOWED_URI_REGEXP:Ki,Xt=typeof u.NAMESPACE=="string"?u.NAMESPACE:mt,At=J(u,"MATHML_TEXT_INTEGRATION_POINTS")&&u.MATHML_TEXT_INTEGRATION_POINTS&&typeof u.MATHML_TEXT_INTEGRATION_POINTS=="object"?ot(u.MATHML_TEXT_INTEGRATION_POINTS):M({},["mi","mo","mn","ms","mtext"]),je=J(u,"HTML_INTEGRATION_POINTS")&&u.HTML_INTEGRATION_POINTS&&typeof u.HTML_INTEGRATION_POINTS=="object"?ot(u.HTML_INTEGRATION_POINTS):M({},["annotation-xml"]);const S=J(u,"CUSTOM_ELEMENT_HANDLING")&&u.CUSTOM_ELEMENT_HANDLING&&typeof u.CUSTOM_ELEMENT_HANDLING=="object"?ot(u.CUSTOM_ELEMENT_HANDLING):he(null);if(j=he(null),J(S,"tagNameCheck")&&Tn(S.tagNameCheck)&&(j.tagNameCheck=S.tagNameCheck),J(S,"attributeNameCheck")&&Tn(S.attributeNameCheck)&&(j.attributeNameCheck=S.attributeNameCheck),J(S,"allowCustomizedBuiltInElements")&&typeof S.allowCustomizedBuiltInElements=="boolean"&&(j.allowCustomizedBuiltInElements=S.allowCustomizedBuiltInElements),Ct&&(Fe=!1),Pt&&(Ot=!0),zt&&(X=M({},Vi),Y=he(null),zt.html===!0&&(M(X,Wi),M(Y,$i)),zt.svg===!0&&(M(X,Hn),M(Y,Vn),M(Y,nn)),zt.svgFilters===!0&&(M(X,jn),M(Y,Vn),M(Y,nn)),zt.mathMl===!0&&(M(X,Wn),M(Y,Yi),M(Y,nn))),vt.tagCheck=null,vt.attributeCheck=null,J(u,"ADD_TAGS")&&(typeof u.ADD_TAGS=="function"?vt.tagCheck=u.ADD_TAGS:ut(u.ADD_TAGS)&&(X===Le&&(X=ot(X)),M(X,u.ADD_TAGS,B))),J(u,"ADD_ATTR")&&(typeof u.ADD_ATTR=="function"?vt.attributeCheck=u.ADD_ATTR:ut(u.ADD_ATTR)&&(Y===me&&(Y=ot(Y)),M(Y,u.ADD_ATTR,B))),J(u,"ADD_URI_SAFE_ATTR")&&ut(u.ADD_URI_SAFE_ATTR)&&M(He,u.ADD_URI_SAFE_ATTR,B),J(u,"FORBID_CONTENTS")&&ut(u.FORBID_CONTENTS)&&(gt===ye&&(gt=ot(gt)),M(gt,u.FORBID_CONTENTS,B)),J(u,"ADD_FORBID_CONTENTS")&&ut(u.ADD_FORBID_CONTENTS)&&(gt===ye&&(gt=ot(gt)),M(gt,u.ADD_FORBID_CONTENTS,B)),Ue&&(X["#text"]=!0),Ft&&M(X,["html","head","body"]),X.table&&(M(X,["tbody"]),delete $t.tbody),u.TRUSTED_TYPES_POLICY){if(typeof u.TRUSTED_TYPES_POLICY.createHTML!="function")throw Ie(\'TRUSTED_TYPES_POLICY configuration option must provide a "createHTML" hook.\');if(typeof u.TRUSTED_TYPES_POLICY.createScriptURL!="function")throw Ie(\'TRUSTED_TYPES_POLICY configuration option must provide a "createScriptURL" hook.\');C=u.TRUSTED_TYPES_POLICY,k=C.createHTML("")}else C===void 0&&(C=ma(b,r)),C!==null&&typeof k=="string"&&(k=C.createHTML(""));(K.uponSanitizeElement.length>0||K.uponSanitizeAttribute.length>0)&&X===Le&&(X=ot(X)),K.uponSanitizeAttribute.length>0&&Y===me&&(Y=ot(Y)),ct&&ct(u),qt=u},wn=M({},[...Hn,...jn,...ra]),Gt=M({},[...Wn,...oa]),ei=function(u){let S=T(u);(!S||!S.tagName)&&(S={namespaceURI:Xt,tagName:"template"});const N=Ne(u.tagName),z=Ne(S.tagName);return Ut[u.namespaceURI]?u.namespaceURI===_e?S.namespaceURI===mt?N==="svg":S.namespaceURI===Bt?N==="svg"&&(z==="annotation-xml"||At[z]):!!wn[N]:u.namespaceURI===Bt?S.namespaceURI===mt?N==="math":S.namespaceURI===_e?N==="math"&&je[z]:!!Gt[N]:u.namespaceURI===mt?S.namespaceURI===_e&&!je[z]||S.namespaceURI===Bt&&!At[z]?!1:!Gt[N]&&(Zn[N]||!wn[N]):!!(re==="application/xhtml+xml"&&Ut[u.namespaceURI]):!1},yt=function(u){de(t.removed,{element:u});try{T(u).removeChild(u)}catch{A(u)}},Ht=function(u,S){try{de(t.removed,{attribute:S.getAttributeNode(u),from:S})}catch{de(t.removed,{attribute:null,from:S})}if(S.removeAttribute(u),u==="is")if(Ot||Pt)try{yt(S)}catch{}else try{S.setAttribute(u,"")}catch{}},bn=function(u){let S=null,N=null;if(Kt)u="<remove></remove>"+u;else{const W=Ui(u,/^[\\r\\n\\t ]+/);N=W&&W[0]}re==="application/xhtml+xml"&&Xt===mt&&(u=\'<html xmlns="http://www.w3.org/1999/xhtml"><head></head><body>\'+u+"</body></html>");const z=C?C.createHTML(u):u;if(Xt===mt)try{S=new f().parseFromString(z,re)}catch{}if(!S||!S.documentElement){S=$.createDocument(Xt,"template",null);try{S.documentElement.innerHTML=Te?k:z}catch{}}const L=S.body||S.documentElement;return u&&N&&L.insertBefore(n.createTextNode(N),L.childNodes[0]||null),Xt===mt?ht.call(S,Ft?"html":"body")[0]:Ft?S.documentElement:L},Sn=function(u){return G.call(u.ownerDocument||u,u,h.SHOW_ELEMENT|h.SHOW_COMMENT|h.SHOW_TEXT|h.SHOW_PROCESSING_INSTRUCTION|h.SHOW_CDATA_SECTION,null)},jt=function(u){u.normalize();const S=G.call(u.ownerDocument||u,u,h.SHOW_TEXT|h.SHOW_COMMENT|h.SHOW_CDATA_SECTION|h.SHOW_PROCESSING_INSTRUCTION,null);let N=S.nextNode();for(;N;){let z=N.data;fe([Vt,Rt,kt],L=>{z=pe(z,L," ")}),N.data=z,N=S.nextNode()}},we=function(u){const S=R?R(u):null;return typeof S!="string"||B(S)!=="form"?!1:typeof u.nodeName!="string"||typeof u.textContent!="string"||typeof u.removeChild!="function"||u.attributes!==E(u)||typeof u.removeAttribute!="function"||typeof u.setAttribute!="function"||typeof u.namespaceURI!="string"||typeof u.insertBefore!="function"||typeof u.hasChildNodes!="function"||u.nodeType!==g(u)||u.childNodes!==_(u)},oe=function(u){if(!g||typeof u!="object"||u===null)return!1;try{return g(u)===It.documentFragment}catch{return!1}},be=function(u){if(!g||typeof u!="object"||u===null)return!1;try{return typeof g(u)=="number"}catch{return!1}};function Et(D,u,S){fe(D,N=>{N.call(t,u,S,qt)})}const vn=function(u){let S=null;if(Et(K.beforeSanitizeElements,u,null),we(u))return yt(u),!0;const N=B(u.nodeName);if(Et(K.uponSanitizeElement,u,{tagName:N,allowedTags:X}),ie&&u.hasChildNodes()&&!be(u.firstElementChild)&&rt(/<[/\\w!]/g,u.innerHTML)&&rt(/<[/\\w!]/g,u.textContent)||ie&&u.namespaceURI===mt&&N==="style"&&be(u.firstElementChild)||u.nodeType===It.progressingInstruction||ie&&u.nodeType===It.comment&&rt(/<[/\\w]/g,u.data))return yt(u),!0;if($t[N]||!(vt.tagCheck instanceof Function&&vt.tagCheck(N))&&!X[N]){if(!$t[N]&&En(N)&&(j.tagNameCheck instanceof RegExp&&rt(j.tagNameCheck,N)||j.tagNameCheck instanceof Function&&j.tagNameCheck(N)))return!1;if(Ue&&!gt[N]){const L=T(u),W=_(u);if(W&&L){const dt=W.length;for(let St=dt-1;St>=0;--St){const _t=p(W[St],!0);L.insertBefore(_t,y(u))}}}return yt(u),!0}return(g?g(u):u.nodeType)===It.element&&!ei(u)||(N==="noscript"||N==="noembed"||N==="noframes")&&rt(/<\\/no(script|embed|frames)/i,u.innerHTML)?(yt(u),!0):(Ct&&u.nodeType===It.text&&(S=u.textContent,fe([Vt,Rt,kt],L=>{S=pe(S,L," ")}),u.textContent!==S&&(de(t.removed,{element:u.cloneNode()}),u.textContent=S)),Et(K.afterSanitizeElements,u,null),!1)},An=function(u,S,N){if(Yt[S]||ze&&(S==="id"||S==="name")&&(N in n||N in ti))return!1;const z=Y[S]||vt.attributeCheck instanceof Function&&vt.attributeCheck(S,u);if(!(Fe&&!Yt[S]&&rt(Me,S))){if(!(ke&&rt(ge,S))){if(!z||Yt[S]){if(!(En(u)&&(j.tagNameCheck instanceof RegExp&&rt(j.tagNameCheck,u)||j.tagNameCheck instanceof Function&&j.tagNameCheck(u))&&(j.attributeNameCheck instanceof RegExp&&rt(j.attributeNameCheck,S)||j.attributeNameCheck instanceof Function&&j.attributeNameCheck(S,u))||S==="is"&&j.allowCustomizedBuiltInElements&&(j.tagNameCheck instanceof RegExp&&rt(j.tagNameCheck,N)||j.tagNameCheck instanceof Function&&j.tagNameCheck(N))))return!1}else if(!He[S]){if(!rt(H,pe(N,pn,""))){if(!((S==="src"||S==="xlink:href"||S==="href")&&u!=="script"&&Gi(N,"data:")===0&&Ge[u])){if(!(gn&&!rt(ne,pe(N,pn,"")))){if(N)return!1}}}}}}return!0},Ve=M({},["annotation-xml","color-profile","font-face","font-face-format","font-face-name","font-face-src","font-face-uri","missing-glyph"]),En=function(u){return!Ve[Ne(u)]&&rt(Xn,u)},se=function(u){Et(K.beforeSanitizeAttributes,u,null);const S=u.attributes;if(!S||we(u))return;const N={attrName:"",attrValue:"",keepAttr:!0,allowedAttributes:Y,forceKeepAttr:void 0};let z=S.length;for(;z--;){const L=S[z],W=L.name,dt=L.namespaceURI,St=L.value,_t=B(W),$e=St;let et=W==="value"?$e:Zs($e);if(N.attrName=_t,N.attrValue=et,N.keepAttr=!0,N.forceKeepAttr=void 0,Et(K.uponSanitizeAttribute,u,N),et=N.attrValue,Be&&(_t==="id"||_t==="name")&&Gi(et,yn)!==0&&(Ht(W,u),et=yn+et),ie&&rt(/((--!?|])>)|<\\/(style|script|title|xmp|textarea|noscript|iframe|noembed|noframes)/i,et)){Ht(W,u);continue}if(_t==="attributename"&&Ui(et,"href")){Ht(W,u);continue}if(N.forceKeepAttr)continue;if(!N.keepAttr){Ht(W,u);continue}if(!mn&&rt(/\\/>/i,et)){Ht(W,u);continue}Ct&&fe([Vt,Rt,kt],Nn=>{et=pe(et,Nn," ")});const xn=B(u.nodeName);if(!An(xn,_t,et)){Ht(W,u);continue}if(C&&typeof b=="object"&&typeof b.getAttributeType=="function"&&!dt)switch(b.getAttributeType(xn,_t)){case"TrustedHTML":{et=C.createHTML(et);break}case"TrustedScriptURL":{et=C.createScriptURL(et);break}}if(et!==$e)try{dt?u.setAttributeNS(dt,W,et):u.setAttribute(W,et),we(u)?yt(u):Bi(t.removed)}catch{Ht(W,u)}}Et(K.afterSanitizeAttributes,u,null)},Zt=function(u){let S=null;const N=Sn(u);for(Et(K.beforeSanitizeShadowDOM,u,null);S=N.nextNode();)if(Et(K.uponSanitizeShadowNode,S,null),vn(S),se(S),oe(S.content)&&Zt(S.content),(g?g(S):S.nodeType)===It.element){const L=v?v(S):S.shadowRoot;oe(L)&&(Qt(L),Zt(L))}Et(K.afterSanitizeShadowDOM,u,null)},Qt=function(u){const S=g?g(u):u.nodeType;if(S===It.element){const L=v?v(u):u.shadowRoot;oe(L)&&(Qt(L),Zt(L))}const N=_?_(u):u.childNodes;if(!N)return;const z=[];fe(N,L=>{de(z,L)});for(const L of z)Qt(L);if(S===It.element){const L=R?R(u):null;if(typeof L=="string"&&B(L)==="template"){const W=u.content;oe(W)&&Qt(W)}}};return t.sanitize=function(D){let u=arguments.length>1&&arguments[1]!==void 0?arguments[1]:{},S=null,N=null,z=null,L=null;if(Te=!D,Te&&(D="<!-->"),typeof D!="string"&&!be(D)&&(D=na(D),typeof D!="string"))throw Ie("dirty is not a string, aborting");if(!t.isSupported)return D;if(Pe||We(u),t.removed=[],typeof D=="string"&&(Mt=!1),Mt){const St=R?R(D):D.nodeName;if(typeof St=="string"){const _t=B(St);if(!X[_t]||$t[_t])throw Ie("root node is forbidden and cannot be sanitized in-place")}if(we(D))throw Ie("root node is clobbered and cannot be sanitized in-place");Qt(D)}else if(be(D))S=bn("<!---->"),N=S.ownerDocument.importNode(D,!0),N.nodeType===It.element&&N.nodeName==="BODY"||N.nodeName==="HTML"?S=N:S.appendChild(N),Qt(N);else{if(!Ot&&!Ct&&!Ft&&D.indexOf("<")===-1)return C&&pt?C.createHTML(D):D;if(S=bn(D),!S)return Ot?null:pt?k:""}S&&Kt&&yt(S.firstChild);const W=Sn(Mt?D:S);for(;z=W.nextNode();)vn(z),se(z),oe(z.content)&&Zt(z.content);if(Mt)return Ct&&jt(D),D;if(Ot){if(Ct&&jt(S),Pt)for(L=Q.call(S.ownerDocument);S.firstChild;)L.appendChild(S.firstChild);else L=S;return(Y.shadowroot||Y.shadowrootmode)&&(L=st.call(i,L,!0)),L}let dt=Ft?S.outerHTML:S.innerHTML;return Ft&&X["!doctype"]&&S.ownerDocument&&S.ownerDocument.doctype&&S.ownerDocument.doctype.name&&rt(da,S.ownerDocument.doctype.name)&&(dt="<!DOCTYPE "+S.ownerDocument.doctype.name+`>\n`+dt),Ct&&fe([Vt,Rt,kt],St=>{dt=pe(dt,St," ")}),C&&pt?C.createHTML(dt):dt},t.setConfig=function(){let D=arguments.length>0&&arguments[0]!==void 0?arguments[0]:{};We(D),Pe=!0},t.clearConfig=function(){qt=null,Pe=!1},t.isValidAttribute=function(D,u,S){qt||We({});const N=B(D),z=B(u);return An(N,z,S)},t.addHook=function(D,u){typeof u=="function"&&de(K[D],u)},t.removeHook=function(D,u){if(u!==void 0){const S=Xs(K[D],u);return S===-1?void 0:qs(K[D],S,1)[0]}return Bi(K[D])},t.removeHooks=function(D){K[D]=[]},t.removeAllHooks=function(){K=Xi()},t}qi();function Zi(e=8,t="id-"){const n="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",i=n+"0123456789-_";let r=n.charAt(Math.floor(Math.random()*n.length));for(let o=1;o<e;o++)r+=i.charAt(Math.floor(Math.random()*i.length));return`${t}${r}`}let Qi=class fr{constructor(t,n,i,r=Zi(),o=[]){x(this,"id");x(this,"data");x(this,"children");x(this,"style");x(this,"edgesOut");x(this,"edgesIn");x(this,"defaultCircleRadius",10);x(this,"x");x(this,"y");x(this,"vx");x(this,"vy");x(this,"fx");x(this,"fy");x(this,"weight");x(this,"frozen");x(this,"visible");x(this,"expanded");x(this,"isChild");x(this,"childrenDepth");x(this,"isParent");x(this,"parentNode");x(this,"_original_object");x(this,"_deepest_node_clone");x(this,"_subgraph");x(this,"_circleRadius",this.defaultCircleRadius);x(this,"_circleRadiusCollapsed",this.defaultCircleRadius);x(this,"_dirty");x(this,"domID");this.id=t,this.domID=r,this.data=n??{},this.style=i??{},this.children=[],this.isParent=!1,this.setChildren(o),this._dirty=!0,this.frozen=!1,this.visible=!0,this.expanded=!1,this.isChild=!1,this.childrenDepth=0,this.edgesOut=new Set,this.edgesIn=new Set}getData(){return this.data}setData(t){this.data=t,this.markDirty()}updateData(t){this.data={...this.data,...t},this.markDirty()}registerEdgeOut(t){this.edgesOut.add(t)}registerEdgeIn(t){this.edgesIn.add(t)}emptyEdges(){this.edgesOut.clear(),this.edgesIn.clear()}getConnectedNodes(){return[...this.edgesOut].map(t=>t.to)}getConnectingNodes(){return[...this.edgesIn].map(t=>t.from)}getEdgesOut(){return[...this.edgesOut]}getEdgesIn(){return[...this.edgesIn]}getStyle(){return this.style}setStyle(t){this.style=t,this.markDirty()}updateStyle(t){this.style={...this.style,...t},this.markDirty()}getGraphElement(){return document?document.getElementById(`node-${this.domID}`):null}toDict(t=!1){const n={id:this.id,data:this.data,style:this.style,weight:this.weight};return t||(n.x=this.x,n.y=this.y,n.vx=this.vx,n.vy=this.vy,n.fx=this.fx,n.fy=this.fy),this.hasChildren()&&(n.children=this.children.map(i=>i.toDict(t))),n}toSimulationDTO(){return{id:this.id,data:this.data,style:this.style,weight:this.weight,_circleRadius:this._circleRadius,x:this.x,y:this.y,vx:this.vx,vy:this.vy,fx:this.fx,fy:this.fy}}clone(){const t={...this.data},n={...this.style},i=new fr(this.id,t,n);return i.x=this.x,i.y=this.y,i.vx=this.vx,i.vy=this.vy,i.fx=this.fx,i.fy=this.fy,i.weight=this.weight,i.frozen=this.frozen,i.visible=this.visible,i.expanded=this.expanded,i.isChild=this.isChild,i.childrenDepth=this.childrenDepth,i.isParent=this.isParent,i.parentNode=this.parentNode,i._circleRadius=this._circleRadius,i.children=this.children.map(r=>r.clone()),i}markDirty(){this._dirty=!0}clearDirty(){this._dirty=!1}isDirty(){return this._dirty}freeze(){this.frozen=!0,this.fx=this.x,this.fy=this.y}unfreeze(){this.frozen=!1,this.fx=void 0,this.fy=void 0}toggleVisibility(t){t?this.show():this.hide(),this.markDirty()}show(){this.visible=!0}hide(){this.visible=!1}toggleExpand(t){t===void 0?this.expanded?this.collapse():this.expand():t?this.expand():this.collapse(),this.markDirty()}expand(){this.expanded=!0,this._original_object&&(this._original_object.expanded=!0)}collapse(){this.expanded=!1,this._original_object&&(this._original_object.expanded=!1)}degree(){return this.edgesOut.size+this.edgesIn.size}setCircleRadius(t){this._circleRadius=t}getCircleRadius(){return this._circleRadius}setCircleRadiusCollapsed(t){this._circleRadiusCollapsed=t}getCircleRadiusCollapsed(){return this._circleRadiusCollapsed}setChildren(t){this.children=t,this.hasChildren()?this.isParent=!0:this.isParent=!1}hasChildren(){return this.children.length>0}markAsChild(t,n){this.isChild=!0,this.childrenDepth=n,this.parentNode=t}markAsParent(){this.isParent=!0}setSubgraph(t){this._subgraph=t}getSubgraph(){return this._subgraph}setOriginalObject(t){this._original_object=t}getOriginalObject(){return this._original_object}setDeepestNodeClone(t){this._deepest_node_clone=t}getDeepestNodeClone(){return this._deepest_node_clone}};class rn{constructor(t,n,i,r,o,a=null,l){x(this,"id");x(this,"from");x(this,"to");x(this,"directed");x(this,"data");x(this,"style");x(this,"visible");x(this,"isSynthetic");x(this,"isCrossCluster");x(this,"syntheticTerminalNode");x(this,"syntheticSourceNode");x(this,"_original_object");x(this,"_subgraphFromNode");x(this,"_subgraphToNode");x(this,"_dirty");x(this,"domID");this.id=t,this.domID=Zi(),this.from=n,this.to=i,this.directed=a,this.data=r??{},this.style=o??{},this.visible=!0,this._dirty=!0,this.isSynthetic=l!==void 0,this.syntheticTerminalNode=l,this.from.registerEdgeOut(this),this.to.registerEdgeIn(this)}get source(){return this.from}get target(){return this.to}getData(){return this.data}setData(t){this.data=t,this.markDirty()}updateData(t){this.data={...this.data,...t},this.markDirty()}getStyle(){return this.style}getEdgeStyle(){var t;return((t=this.style)==null?void 0:t.edge)??{}}getLabelStyle(){var t;return((t=this.style)==null?void 0:t.label)??{}}setStyle(t){this.style=t,this.markDirty()}updateStyle(t){const n=this.style,i=t;this.style={...n,...i,edge:{...n.edge,...i.edge},label:{...n.label,...i.label}},this.markDirty()}getGraphElement(){return document?document.getElementById(`edge-${this.domID}`):null}setFrom(t){this.from=t}setTo(t){this.to=t}toDict(){return{id:this.id,from:this.from.id,to:this.to.id,data:this.data,style:this.style}}toSimulationDTO(){return{id:this.id,from:{id:this.from.id},to:{id:this.to.id},data:this.data,style:this.style,directed:this.directed}}clone(){const t={...this.data},n={...this.style},i=new rn(this.id,this.from.clone(),this.to.clone(),t,n,this.directed);return i.visible=this.visible,i}markDirty(){this._dirty=!0}clearDirty(){this._dirty=!1}isDirty(){return this._dirty}toggleVisibility(t){t?this.show():this.hide(),this.markDirty()}show(){this.visible=!0}hide(){this.visible=!1}setOriginalObject(t){this._original_object=t}getOriginalObject(){return this._original_object}setSubgraphFromNode(t){this._subgraphFromNode=t}setSubgraphToNode(t){this._subgraphToNode=t}getSubgraphFromNode(){return this._subgraphFromNode}getSubgraphToNode(){return this._subgraphToNode}}function ya(e){return new Worker(self.location.href,{name:e==null?void 0:e.name})}function _a(){return new ya}const Ta=(e,t,n,i,r)=>new Promise((o,a)=>{const l=_a();l.postMessage({source:"simulation-worker-wrapper",nodes:e,edges:t,options:n,canvasBCR:i}),l.onmessage=h=>{const{type:s,progress:f,nodes:b,edges:m,elapsedTime:p}=h.data;if(s==="tick"&&typeof f=="number"){r==null||r(f,p);return}s==="done"&&(o({nodes:b,edges:m}),l.terminate())},l.onerror=a});var on=typeof globalThis<"u"?globalThis:typeof window<"u"?window:typeof global<"u"?global:typeof self<"u"?self:{};function wa(e){return e&&e.__esModule&&Object.prototype.hasOwnProperty.call(e,"default")?e.default:e}var Re={exports:{}};Re.exports;var Ji;function ba(){return Ji||(Ji=1,(function(e,t){var n=200,i="__lodash_hash_undefined__",r=800,o=16,a=9007199254740991,l="[object Arguments]",h="[object Array]",s="[object AsyncFunction]",f="[object Boolean]",b="[object Date]",m="[object Error]",p="[object Function]",A="[object GeneratorFunction]",y="[object Map]",_="[object Number]",T="[object Null]",v="[object Object]",E="[object Proxy]",g="[object RegExp]",R="[object Set]",C="[object String]",k="[object Undefined]",F="[object WeakMap]",$="[object ArrayBuffer]",G="[object DataView]",Q="[object Float32Array]",ht="[object Float64Array]",st="[object Int8Array]",K="[object Int16Array]",Vt="[object Int32Array]",Rt="[object Uint8Array]",kt="[object Uint8ClampedArray]",Me="[object Uint16Array]",ge="[object Uint32Array]",ne=/[\\\\^$.*+?()[\\]{}|]/g,pn=/^\\[object .+?Constructor\\]$/,Xn=/^(?:0|[1-9]\\d*)$/,H={};H[Q]=H[ht]=H[st]=H[K]=H[Vt]=H[Rt]=H[kt]=H[Me]=H[ge]=!0,H[l]=H[h]=H[$]=H[f]=H[G]=H[b]=H[m]=H[p]=H[y]=H[_]=H[v]=H[g]=H[R]=H[C]=H[F]=!1;var X=typeof on=="object"&&on&&on.Object===Object&&on,Le=typeof self=="object"&&self&&self.Object===Object&&self,Y=X||Le||Function("return this")(),me=t&&!t.nodeType&&t,j=me&&!0&&e&&!e.nodeType&&e,$t=j&&j.exports===me,Yt=$t&&X.process,vt=(function(){try{var c=j&&j.require&&j.require("util").types;return c||Yt&&Yt.binding&&Yt.binding("util")}catch{}})(),ke=vt&&vt.isTypedArray;function Fe(c,d,w){switch(w.length){case 0:return c.call(d);case 1:return c.call(d,w[0]);case 2:return c.call(d,w[0],w[1]);case 3:return c.call(d,w[0],w[1],w[2])}return c.apply(d,w)}function gn(c,d){for(var w=-1,I=Array(c);++w<c;)I[w]=d(w);return I}function mn(c){return function(d){return c(d)}}function Ct(c,d){return c==null?void 0:c[d]}function ie(c,d){return function(w){return c(d(w))}}var Ft=Array.prototype,Pe=Function.prototype,Kt=Object.prototype,Ot=Y["__core-js_shared__"],Pt=Pe.toString,pt=Kt.hasOwnProperty,ze=(function(){var c=/[^.]+$/.exec(Ot&&Ot.keys&&Ot.keys.IE_PROTO||"");return c?"Symbol(src)_1."+c:""})(),Be=Kt.toString,yn=Pt.call(Object),Ue=RegExp("^"+Pt.call(pt).replace(ne,"\\\\$&").replace(/hasOwnProperty|(function).*?(?=\\\\\\()| for .+?(?=\\\\\\])/g,"$1.*?")+"$"),Mt=$t?Y.Buffer:void 0,zt=Y.Symbol,gt=Y.Uint8Array;Mt&&Mt.allocUnsafe;var ye=ie(Object.getPrototypeOf,Object),Ge=Object.create,_n=Kt.propertyIsEnumerable,He=Ft.splice,Lt=zt?zt.toStringTag:void 0,Bt=(function(){try{var c=ni(Object,"defineProperty");return c({},"",{}),c}catch{}})(),_e=Mt?Mt.isBuffer:void 0,mt=Math.max,Xt=Date.now,Te=ni(Y,"Map"),Ut=ni(Object,"create"),qn=(function(){function c(){}return function(d){if(!ae(d))return{};if(Ge)return Ge(d);c.prototype=d;var w=new c;return c.prototype=void 0,w}})();function At(c){var d=-1,w=c==null?0:c.length;for(this.clear();++d<w;){var I=c[d];this.set(I[0],I[1])}}function je(){this.__data__=Ut?Ut(null):{},this.size=0}function Zn(c){var d=this.has(c)&&delete this.__data__[c];return this.size-=d?1:0,d}function re(c){var d=this.__data__;if(Ut){var w=d[c];return w===i?void 0:w}return pt.call(d,c)?d[c]:void 0}function Qn(c){var d=this.__data__;return Ut?d[c]!==void 0:pt.call(d,c)}function Jn(c,d){var w=this.__data__;return this.size+=this.has(c)?0:1,w[c]=Ut&&d===void 0?i:d,this}At.prototype.clear=je,At.prototype.delete=Zn,At.prototype.get=re,At.prototype.has=Qn,At.prototype.set=Jn;function B(c){var d=-1,w=c==null?0:c.length;for(this.clear();++d<w;){var I=c[d];this.set(I[0],I[1])}}function qt(){this.__data__=[],this.size=0}function ti(c){var d=this.__data__,w=se(d,c);if(w<0)return!1;var I=d.length-1;return w==I?d.pop():He.call(d,w,1),--this.size,!0}function Tn(c){var d=this.__data__,w=se(d,c);return w<0?void 0:d[w][1]}function We(c){return se(this.__data__,c)>-1}function wn(c,d){var w=this.__data__,I=se(w,c);return I<0?(++this.size,w.push([c,d])):w[I][1]=d,this}B.prototype.clear=qt,B.prototype.delete=ti,B.prototype.get=Tn,B.prototype.has=We,B.prototype.set=wn;function Gt(c){var d=-1,w=c==null?0:c.length;for(this.clear();++d<w;){var I=c[d];this.set(I[0],I[1])}}function ei(){this.size=0,this.__data__={hash:new At,map:new(Te||B),string:new At}}function yt(c){var d=Dn(this,c).delete(c);return this.size-=d?1:0,d}function Ht(c){return Dn(this,c).get(c)}function bn(c){return Dn(this,c).has(c)}function Sn(c,d){var w=Dn(this,c),I=w.size;return w.set(c,d),this.size+=w.size==I?0:1,this}Gt.prototype.clear=ei,Gt.prototype.delete=yt,Gt.prototype.get=Ht,Gt.prototype.has=bn,Gt.prototype.set=Sn;function jt(c){var d=this.__data__=new B(c);this.size=d.size}function we(){this.__data__=new B,this.size=0}function oe(c){var d=this.__data__,w=d.delete(c);return this.size=d.size,w}function be(c){return this.__data__.get(c)}function Et(c){return this.__data__.has(c)}function vn(c,d){var w=this.__data__;if(w instanceof B){var I=w.__data__;if(!Te||I.length<n-1)return I.push([c,d]),this.size=++w.size,this;w=this.__data__=new Gt(I)}return w.set(c,d),this.size=w.size,this}jt.prototype.clear=we,jt.prototype.delete=oe,jt.prototype.get=be,jt.prototype.has=Et,jt.prototype.set=vn;function An(c,d){var w=oi(c),I=!w&&ri(c),P=!w&&!I&&sr(c),V=!w&&!I&&!P&&lr(c),q=w||I||P||V,U=q?gn(c.length,String):[],Z=U.length;for(var xt in c)q&&(xt=="length"||P&&(xt=="offset"||xt=="parent")||V&&(xt=="buffer"||xt=="byteLength"||xt=="byteOffset")||rr(xt,Z))||U.push(xt);return U}function Ve(c,d,w){(w!==void 0&&!In(c[d],w)||w===void 0&&!(d in c))&&Zt(c,d,w)}function En(c,d,w){var I=c[d];(!(pt.call(c,d)&&In(I,w))||w===void 0&&!(d in c))&&Zt(c,d,w)}function se(c,d){for(var w=c.length;w--;)if(In(c[w][0],d))return w;return-1}function Zt(c,d,w){d=="__proto__"&&Bt?Bt(c,d,{configurable:!0,enumerable:!0,value:w,writable:!0}):c[d]=w}var Qt=il();function D(c){return c==null?c===void 0?k:T:Lt&&Lt in Object(c)?rl(c):ul(c)}function u(c){return Ye(c)&&D(c)==l}function S(c){if(!ae(c)||ll(c))return!1;var d=ai(c)?Ue:pn;return d.test(pl(c))}function N(c){return Ye(c)&&ar(c.length)&&!!H[D(c)]}function z(c){if(!ae(c))return cl(c);var d=or(c),w=[];for(var I in c)I=="constructor"&&(d||!pt.call(c,I))||w.push(I);return w}function L(c,d,w,I,P){c!==d&&Qt(d,function(V,q){if(P||(P=new jt),ae(V))W(c,d,q,w,L,I,P);else{var U=I?I(ii(c,q),V,q+"",c,d,P):void 0;U===void 0&&(U=V),Ve(c,q,U)}},cr)}function W(c,d,w,I,P,V,q){var U=ii(c,w),Z=ii(d,w),xt=q.get(Z);if(xt){Ve(c,w,xt);return}var Tt=V?V(U,Z,w+"",c,d,q):void 0,Ke=Tt===void 0;if(Ke){var li=oi(Z),ci=!li&&sr(Z),hr=!li&&!ci&&lr(Z);Tt=Z,li||ci||hr?oi(U)?Tt=U:gl(U)?Tt=xn(U):ci?(Ke=!1,Tt=_t(Z)):hr?(Ke=!1,Tt=et(Z)):Tt=[]:ml(Z)||ri(Z)?(Tt=U,ri(U)?Tt=yl(U):(!ae(U)||ai(U))&&(Tt=ol(Z))):Ke=!1}Ke&&(q.set(Z,Tt),P(Tt,Z,I,V,q),q.delete(Z)),Ve(c,w,Tt)}function dt(c,d){return fl(hl(c,d,ur),c+"")}var St=Bt?function(c,d){return Bt(c,"toString",{configurable:!0,enumerable:!1,value:Tl(d),writable:!0})}:ur;function _t(c,d){return c.slice()}function $e(c){var d=new c.constructor(c.byteLength);return new gt(d).set(new gt(c)),d}function et(c,d){var w=$e(c.buffer);return new c.constructor(w,c.byteOffset,c.length)}function xn(c,d){var w=-1,I=c.length;for(d||(d=Array(I));++w<I;)d[w]=c[w];return d}function Nn(c,d,w,I){var P=!w;w||(w={});for(var V=-1,q=d.length;++V<q;){var U=d[V],Z=void 0;Z===void 0&&(Z=c[U]),P?Zt(w,U,Z):En(w,U,Z)}return w}function nl(c){return dt(function(d,w){var I=-1,P=w.length,V=P>1?w[P-1]:void 0,q=P>2?w[2]:void 0;for(V=c.length>3&&typeof V=="function"?(P--,V):void 0,q&&sl(w[0],w[1],q)&&(V=P<3?void 0:V,P=1),d=Object(d);++I<P;){var U=w[I];U&&c(d,U,I,V)}return d})}function il(c){return function(d,w,I){for(var P=-1,V=Object(d),q=I(d),U=q.length;U--;){var Z=q[++P];if(w(V[Z],Z,V)===!1)break}return d}}function Dn(c,d){var w=c.__data__;return al(d)?w[typeof d=="string"?"string":"hash"]:w.map}function ni(c,d){var w=Ct(c,d);return S(w)?w:void 0}function rl(c){var d=pt.call(c,Lt),w=c[Lt];try{c[Lt]=void 0;var I=!0}catch{}var P=Be.call(c);return I&&(d?c[Lt]=w:delete c[Lt]),P}function ol(c){return typeof c.constructor=="function"&&!or(c)?qn(ye(c)):{}}function rr(c,d){var w=typeof c;return d=d??a,!!d&&(w=="number"||w!="symbol"&&Xn.test(c))&&c>-1&&c%1==0&&c<d}function sl(c,d,w){if(!ae(w))return!1;var I=typeof d;return(I=="number"?si(w)&&rr(d,w.length):I=="string"&&d in w)?In(w[d],c):!1}function al(c){var d=typeof c;return d=="string"||d=="number"||d=="symbol"||d=="boolean"?c!=="__proto__":c===null}function ll(c){return!!ze&&ze in c}function or(c){var d=c&&c.constructor,w=typeof d=="function"&&d.prototype||Kt;return c===w}function cl(c){var d=[];if(c!=null)for(var w in Object(c))d.push(w);return d}function ul(c){return Be.call(c)}function hl(c,d,w){return d=mt(d===void 0?c.length-1:d,0),function(){for(var I=arguments,P=-1,V=mt(I.length-d,0),q=Array(V);++P<V;)q[P]=I[d+P];P=-1;for(var U=Array(d+1);++P<d;)U[P]=I[P];return U[d]=w(q),Fe(c,this,U)}}function ii(c,d){if(!(d==="constructor"&&typeof c[d]=="function")&&d!="__proto__")return c[d]}var fl=dl(St);function dl(c){var d=0,w=0;return function(){var I=Xt(),P=o-(I-w);if(w=I,P>0){if(++d>=r)return arguments[0]}else d=0;return c.apply(void 0,arguments)}}function pl(c){if(c!=null){try{return Pt.call(c)}catch{}try{return c+""}catch{}}return""}function In(c,d){return c===d||c!==c&&d!==d}var ri=u((function(){return arguments})())?u:function(c){return Ye(c)&&pt.call(c,"callee")&&!_n.call(c,"callee")},oi=Array.isArray;function si(c){return c!=null&&ar(c.length)&&!ai(c)}function gl(c){return Ye(c)&&si(c)}var sr=_e||wl;function ai(c){if(!ae(c))return!1;var d=D(c);return d==p||d==A||d==s||d==E}function ar(c){return typeof c=="number"&&c>-1&&c%1==0&&c<=a}function ae(c){var d=typeof c;return c!=null&&(d=="object"||d=="function")}function Ye(c){return c!=null&&typeof c=="object"}function ml(c){if(!Ye(c)||D(c)!=v)return!1;var d=ye(c);if(d===null)return!0;var w=pt.call(d,"constructor")&&d.constructor;return typeof w=="function"&&w instanceof w&&Pt.call(w)==yn}var lr=ke?mn(ke):N;function yl(c){return Nn(c,cr(c))}function cr(c){return si(c)?An(c):z(c)}var _l=nl(function(c,d,w){L(c,d,w)});function Tl(c){return function(){return c}}function ur(c){return c}function wl(){return!1}e.exports=_l})(Re,Re.exports)),Re.exports}var Sa=ba(),sn=wa(Sa);function va(e){var t=0,n=e.children,i=n&&n.length;if(!i)t=1;else for(;--i>=0;)t+=n[i].value;e.value=t}function Aa(){return this.eachAfter(va)}function Ea(e,t){let n=-1;for(const i of this)e.call(t,i,++n,this);return this}function xa(e,t){for(var n=this,i=[n],r,o,a=-1;n=i.pop();)if(e.call(t,n,++a,this),r=n.children)for(o=r.length-1;o>=0;--o)i.push(r[o]);return this}function Na(e,t){for(var n=this,i=[n],r=[],o,a,l,h=-1;n=i.pop();)if(r.push(n),o=n.children)for(a=0,l=o.length;a<l;++a)i.push(o[a]);for(;n=r.pop();)e.call(t,n,++h,this);return this}function Da(e,t){let n=-1;for(const i of this)if(e.call(t,i,++n,this))return i}function Ia(e){return this.eachAfter(function(t){for(var n=+e(t.data)||0,i=t.children,r=i&&i.length;--r>=0;)n+=i[r].value;t.value=n})}function Ra(e){return this.eachBefore(function(t){t.children&&t.children.sort(e)})}function Ca(e){for(var t=this,n=Oa(t,e),i=[t];t!==n;)t=t.parent,i.push(t);for(var r=i.length;e!==n;)i.splice(r,0,e),e=e.parent;return i}function Oa(e,t){if(e===t)return e;var n=e.ancestors(),i=t.ancestors(),r=null;for(e=n.pop(),t=i.pop();e===t;)r=e,e=n.pop(),t=i.pop();return r}function Ma(){for(var e=this,t=[e];e=e.parent;)t.push(e);return t}function La(){return Array.from(this)}function ka(){var e=[];return this.eachBefore(function(t){t.children||e.push(t)}),e}function Fa(){var e=this,t=[];return e.each(function(n){n!==e&&t.push({source:n.parent,target:n})}),t}function*Pa(){var e=this,t,n=[e],i,r,o;do for(t=n.reverse(),n=[];e=t.pop();)if(yield e,i=e.children)for(r=0,o=i.length;r<o;++r)n.push(i[r]);while(n.length)}function an(e,t){e instanceof Map?(e=[void 0,e],t===void 0&&(t=Ua)):t===void 0&&(t=Ba);for(var n=new Ce(e),i,r=[n],o,a,l,h;i=r.pop();)if((a=t(i.data))&&(h=(a=Array.from(a)).length))for(i.children=a,l=h-1;l>=0;--l)r.push(o=a[l]=new Ce(a[l])),o.parent=i,o.depth=i.depth+1;return n.eachBefore(Ha)}function za(){return an(this).eachBefore(Ga)}function Ba(e){return e.children}function Ua(e){return Array.isArray(e)?e[1]:null}function Ga(e){e.data.value!==void 0&&(e.value=e.data.value),e.data=e.data.data}function Ha(e){var t=0;do e.height=t;while((e=e.parent)&&e.height<++t)}function Ce(e){this.data=e,this.depth=this.height=0,this.parent=null}Ce.prototype=an.prototype={constructor:Ce,count:Aa,each:Ea,eachAfter:Na,eachBefore:xa,find:Da,sum:Ia,sort:Ra,path:Ca,ancestors:Ma,descendants:La,leaves:ka,links:Fa,copy:za,[Symbol.iterator]:Pa};function ja(e,t){return e.parent===t.parent?1:2}function $n(e){var t=e.children;return t?t[0]:e.t}function Yn(e){var t=e.children;return t?t[t.length-1]:e.t}function Wa(e,t,n){var i=n/(t.i-e.i);t.c-=i,t.s+=n,e.c+=i,t.z+=n,t.m+=n}function Va(e){for(var t=0,n=0,i=e.children,r=i.length,o;--r>=0;)o=i[r],o.z+=t,o.m+=t,t+=o.s+(n+=o.c)}function $a(e,t,n){return e.a.parent===t.parent?e.a:n}function ln(e,t){this._=e,this.parent=null,this.children=null,this.A=null,this.a=this,this.z=0,this.m=0,this.c=0,this.s=0,this.t=null,this.i=t}ln.prototype=Object.create(Ce.prototype);function Ya(e){for(var t=new ln(e,0),n,i=[t],r,o,a,l;n=i.pop();)if(o=n._.children)for(n.children=new Array(l=o.length),a=l-1;a>=0;--a)i.push(r=n.children[a]=new ln(o[a],a)),r.parent=n;return(t.parent=new ln(null,0)).children=[t],t}function tr(){var e=ja,t=1,n=1,i=null;function r(s){var f=Ya(s);if(f.eachAfter(o),f.parent.m=-f.z,f.eachBefore(a),i)s.eachBefore(h);else{var b=s,m=s,p=s;s.eachBefore(function(v){v.x<b.x&&(b=v),v.x>m.x&&(m=v),v.depth>p.depth&&(p=v)});var A=b===m?1:e(b,m)/2,y=A-b.x,_=t/(m.x+A+y),T=n/(p.depth||1);s.eachBefore(function(v){v.x=(v.x+y)*_,v.y=v.depth*T})}return s}function o(s){var f=s.children,b=s.parent.children,m=s.i?b[s.i-1]:null;if(f){Va(s);var p=(f[0].z+f[f.length-1].z)/2;m?(s.z=m.z+e(s._,m._),s.m=s.z-p):s.z=p}else m&&(s.z=m.z+e(s._,m._));s.parent.A=l(s,m,s.parent.A||b[0])}function a(s){s._.x=s.z+s.parent.m,s.m+=s.parent.m}function l(s,f,b){if(f){for(var m=s,p=s,A=f,y=m.parent.children[0],_=m.m,T=p.m,v=A.m,E=y.m,g;A=Yn(A),m=$n(m),A&&m;)y=$n(y),p=Yn(p),p.a=s,g=A.z+v-m.z-_+e(A._,m._),g>0&&(Wa($a(A,s,b),s,g),_+=g,T+=g),v+=A.m,_+=m.m,E+=y.m,T+=p.m;A&&!Yn(p)&&(p.t=A,p.m+=v-T),m&&!$n(y)&&(y.t=m,y.m+=_-E,b=s)}return b}function h(s){s.x*=t,s.y=s.depth*n}return r.separation=function(s){return arguments.length?(e=s,r):e},r.size=function(s){return arguments.length?(i=!1,t=+s[0],n=+s[1],r):i?null:[t,n]},r.nodeSize=function(s){return arguments.length?(i=!0,t=+s[0],n=+s[1],r):i?[t,n]:null},r}function cn(e,t){const n=new Map;for(const a of e)n.set(a.id,[]);for(const{source:a,target:l}of t){const h=n.get(a.id);h?h.push(l.id):n.set(a.id,[l.id])}const i=new Set,r=new Set,o=[];for(const a of e)if(!i.has(a.id))for(i.add(a.id),r.add(a.id),o.push({id:a.id,next:0});o.length>0;){const l=o[o.length-1],h=n.get(l.id)??[];if(l.next>=h.length){r.delete(l.id),o.pop();continue}const s=h[l.next++];if(r.has(s))return!0;i.has(s)||(i.add(s),r.add(s),o.push({id:s,next:0}))}return!1}function er(e,t){const n=new Set(t.map(i=>i.target.id));for(const i of e)if(!n.has(i.id))return i;return e[0]}const Ka=1e6;function Xa(e,t){var l;const n=new Map;for(const h of e)n.set(h.id,[]);for(const h of t)(l=n.get(h.from.id))==null||l.push(h.to);let i=0,r=!1,o=null,a=-1;for(const h of e){const s=new Set([h.id]),f=[h];for(;f.length>0&&!r;){const m=f.pop();for(const p of n.get(m.id)??[]){if(++i>Ka){r=!0;break}s.has(p.id)||(s.add(p.id),f.push(p))}}const b=s.size-1;if(b>a&&(a=b,o=h),r)break}return r&&console.warn("Pivotick: reachability search hit its traversal cap, using the best root found so far."),o??e[0]}function qa(e,t){const n=new Map,i=new Map;for(const s of e)n.set(s.id,[]),i.set(s.id,0);for(const s of t)s.directed!==!1&&(n.get(s.from.id).push(s.to),i.set(s.to.id,(i.get(s.to.id)||0)+1));const r=[],o=e.filter(s=>i.get(s.id)===0);for(;o.length;){const s=o.shift();r.push(s);for(const f of n.get(s.id))i.set(f.id,i.get(f.id)-1),i.get(f.id)===0&&o.push(f)}if(r.length!==e.length)return console.warn("Graph has a cycle! Min-max distance root undefined."),e[0];const a=new Map;for(let s=r.length-1;s>=0;s--){const f=r[s];let b=0;for(const m of n.get(f.id))b=Math.max(b,1+(a.get(m.id)||0));a.set(f.id,b)}let l=null,h=1/0;for(const s of e){const f=a.get(s.id);f<h&&(h=f,l=s)}return l??e[0]}function Za(e,t){const n=new Map,i=new Map;for(const s of e)n.set(s.id,[]),i.set(s.id,0);for(const s of t)s.directed!==!1&&(n.get(s.from.id).push(s.to),i.set(s.to.id,(i.get(s.to.id)||0)+1));const r=[],o=e.filter(s=>i.get(s.id)===0);for(;o.length;){const s=o.shift();r.push(s);for(const f of n.get(s.id))i.set(f.id,i.get(f.id)-1),i.get(f.id)===0&&o.push(f)}if(r.length!==e.length)return console.warn("Graph has a cycle! Cannot minimize DAG height."),e[0];const a=new Map;for(let s=r.length-1;s>=0;s--){const f=r[s];let b=0;for(const m of n.get(f.id))b=Math.max(b,1+(a.get(m.id)??0));a.set(f.id,b)}let l=null,h=1/0;for(const s of e){const f=a.get(s.id);f<h&&(h=f,l=s)}return l??e[0]}const Kn={type:"tree",rootId:void 0,rootIdAlgorithmFinder:"MaxReachability",strength:.25,radial:!1,radialGap:750,horizontal:!1,flipEdgeDirection:!1};class it{constructor(t,n,i,r={}){x(this,"graph");x(this,"simulation");x(this,"simulationForces");x(this,"options");x(this,"originalForceStrength");x(this,"canvasBCR");x(this,"levels");x(this,"positionedNodesByID");this.graph=t,this.simulation=n,this.simulationForces=i,this.options=sn({},Kn,r),this.originalForceStrength={link:this.simulationForces.link.strength(),charge:this.simulationForces.charge.strength(),gravity:this.simulationForces.gravity.strength()},this.positionedNodesByID=new Map,this.levels=new Map;const o=this.graph.getNodes(),a=this.options.flipEdgeDirection?this.flipEdgeDirection(this.graph.getEdges()):this.graph.getEdges();if(cn(o,a)){this.graph.notifier.warning("Tree layout unavailable","The graph contains a cycle, so it cannot be displayed as a tree.");return}this.setSizes(),this.update(),this.registerForces()}update(){const t=this.graph.getNodes(),n=this.options.flipEdgeDirection?this.flipEdgeDirection(this.graph.getEdges()):this.graph.getEdges(),{levels:i}=this.buildLevels(t,n,void 0,this.options.rootIdAlgorithmFinder),{nodes:r,nodeById:o}=this.buildTree(t,n,this.options,this.canvasBCR);this.positionedNodesByID=o,this.levels=i,r&&this.setNodePositions(r,this.options)}flipEdgeDirection(t){return t.forEach(n=>{const i=n.from;n.setFrom(n.to),n.setTo(i)}),t}setSizes(){const t=this.graph.renderer.getCanvas();if(!t)throw new Error("Canvas element is not defined in the graph renderer.");this.canvasBCR=t.getBoundingClientRect()}setNodePositions(t,n){for(const i of t){const r=this.graph.getMutableNode(i.data.id);if(r)if(n.radial){const o=i.x??0,a=i.y??0;r.x=a*Math.cos(o-Math.PI/2),r.y=a*Math.sin(o-Math.PI/2),r.fx=r.x,r.fy=r.y}else n.horizontal?(r.x=i.y,r.fx=i.y,r.y=i.x,delete r.fy):(r.x=i.x,r.y=i.y,r.fy=i.y,delete r.fx)}}unsetNodePositions(){this.graph.getMutableNodes().forEach(t=>{delete t.fy,delete t.fx})}registerForces(){const t=this.options.strength??.1;if(this.options.radial){const n=Ti(i=>(this.levels.get(i.id)??1)*100,0,0).strength(t);this.simulation.force("tree-radial",n)}else this.simulation.force("tree-y",bi(n=>{var i,r;return this.options.horizontal?((i=this.positionedNodesByID.get(n.id))==null?void 0:i.x)??0:((r=this.positionedNodesByID.get(n.id))==null?void 0:r.y)??0}).strength(t)),this.simulation.force("tree-x",wi(n=>{var i,r;return this.options.horizontal?((i=this.positionedNodesByID.get(n.id))==null?void 0:i.y)??0:((r=this.positionedNodesByID.get(n.id))==null?void 0:r.x)??0}).strength(t));it.adjustOtherSimulationForces(this.simulationForces,this.options)}unregisterLayout(){this.unregisterForces(),this.unsetNodePositions()}unregisterForces(){this.simulation.force("tree-radial",null),this.simulation.force("tree-y",null),this.simulation.force("tree-x",null),it.resetOtherSimulationForces(this.simulationForces,this.originalForceStrength)}static registerForcesOnSimulation(t,n,i,r,o,a,l=this){const h=sn({},Kn,o),s=h.strength??.1,f=a.width,b=a.height,m=[f/2,b/2];if(cn(t,n))return;const{levels:p}=l.buildLevelsStatic(t,n,void 0,h.rootIdAlgorithmFinder),{nodeById:A}=l.buildTreeStatic(t,n,h,a);if(h.radial){const y=Ti(_=>(p.get(_.id)??1)*100,m[0],m[1]).strength(s);i.force("tree-radial",y)}else i.force("tree-y",bi(y=>{var _,T;return h.horizontal?((_=A.get(y.id))==null?void 0:_.x)??0:((T=A.get(y.id))==null?void 0:T.y)??0}).strength(s)),i.force("tree-x",wi(y=>{var _,T;return h.horizontal?((_=A.get(y.id))==null?void 0:_.y)??0:((T=A.get(y.id))==null?void 0:T.x)??0}).strength(s));l.adjustOtherSimulationForces(r,h)}static adjustOtherSimulationForces(t,n){n!=null&&n.radial?(t.link.strength(0),t.charge.strength(0),t.gravity.strength(0)):(t.link.strength(0),t.charge.strength(0),t.gravity.strength(1e-5))}static resetOtherSimulationForces(t,n){t.link.strength(n.link),t.charge.strength(n.charge),t.gravity.strength(n.gravity)}static simulationDone(t,n,i,r){const o=sn({},Kn,r);for(const a of t)o.radial?(a.fx=a.x,a.fy=a.y):o.horizontal?(a.fx=a.x,delete a.fy):(a.fy=a.y,delete a.fx)}buildTree(t,n,i,r){return it.buildTreeStatic(t,n,i,r)}static buildTreeStatic(t,n,i,r){if(!t.length)return{root:null,nodes:[],nodeById:new Map};if(cn(t,n))return console.warn("Cycle detected in graph. Tree layout will not be computed."),{root:null,nodes:[],nodeById:new Map};const o=new Map;for(const y of t){const _=y;_.children=[],o.set(y.id,_)}for(const y of n){const _=o.get(y.source.id),T=o.get(y.target.id);_&&T&&(_.children.push(T),T.parent=_)}const a=i.rootId||it.findRootId(t,n,i.rootIdAlgorithmFinder),l=o.get(a);if(!l)throw new Error(`Root node with id "${a}" not found.`);const h=i.radialGap,s=i.radial?2*Math.PI:r.width,f=i.radial?h:r.height,b=tr();i.radial?b.size([s,f]):b.size([s,f]).separation((y,_)=>{var v,E;const T=((E=(v=y.parent)==null?void 0:v.children)==null?void 0:E.length)??1;return y.parent===_.parent?1.5/T:1.5});const m=an(l),p=b(m),A=new Map;return p.descendants().forEach(y=>{A.set(y.data.id,y)}),{root:p,nodes:p.descendants(),nodeById:A}}buildLevels(t,n,i,r){return it.buildLevelsStatic(t,n,i,r)}static buildLevelsStatic(t,n,i,r){var m;if(!t.length)return{levels:new Map,maxDepth:0,nodeCountPerLevel:{}};const o=i||it.findRootId(t,n,r),a=new Map([[o,0]]),l=new Map;for(const p of t)l.set(p.id,[]);for(const{source:p,target:A}of n)(m=l.get(p.id))==null||m.push(A.id);const h=[o];let s=0;for(;s<h.length;){const p=h[s++],A=a.get(p)??0;for(const y of l.get(p)??[])a.has(y)||(a.set(y,A+1),h.push(y))}let f=0;const b={};for(const p of a.values())p>f&&(f=p),b[p]=(b[p]||0)+1;return{levels:a,maxDepth:f,nodeCountPerLevel:b}}static findRootId(t,n,i){switch(i){case"FirstZeroInDegree":return er(t,n).id;case"MaxReachability":return Xa(t,n).id;case"MinMaxDistance":return qa(t,n).id;case"MinHeight":return Za(t,n).id;default:return er(t,n).id}}}class Oe extends it{constructor(t,n,i,r){super(t,n,i,{...r,type:"tree"})}static registerForcesOnSimulation(t,n,i,r,o,a){it.registerForcesOnSimulation(t,n,i,r,o,a,Oe)}buildTree(t,n,i,r){return Oe.buildTreeStatic(t,n,i,r)}static buildTreeStatic(t,n,i,r){if(!t.length)return{root:null,nodes:[],nodeById:new Map};if(cn(t,n))return console.warn("Cycle detected in graph. Tree layout will not be computed."),{root:null,nodes:[],nodeById:new Map};const o=new Map;for(const y of t){const _=y;_.children=[],o.set(y.id,_)}if(!i.rootId||!o.get(i.rootId))throw new Error("Ego Tree can only be created with a rootId");const a=i.rootId,l=o.get(a);if(l.children=[],!l)throw new Error(`Root node with id "${a}" not found.`);for(const y of n){const _=o.get(y.source.id),T=o.get(y.target.id);_&&T&&(y.source.id===l.id?(l.children.push(T),T.parent=l):y.target.id===l.id&&(l.children.push(_),_.parent=l))}const h=i.radialGap,s=i.radial?2*Math.PI:r.width,f=i.radial?h:r.height,b=tr();i.radial?b.size([s,f]):b.size([s,f]).separation((y,_)=>{var v,E;const T=((E=(v=y.parent)==null?void 0:v.children)==null?void 0:E.length)??1;return y.parent===_.parent?1.5/T:1.5});const m=an(l),p=b(m),A=new Map;return p.descendants().forEach(y=>{A.set(y.data.id,y)}),{root:p,nodes:p.descendants(),nodeById:A}}}function Qa(e){var n;const t=(n=e.getData())==null?void 0:n.label;return typeof t=="string"?t:""}const ee={d3Alpha:1,d3AlphaMin:.001,d3AlphaDecay:.05,d3AlphaTarget:0,d3VelocityDecay:.45,d3LinkDistance:40,d3LinkStrength:null,d3ManyBodyStrength:-150,d3ManyBodyTheta:.9,d3CollideRadius:12,d3CollideRadiusMultiplier:1.2,d3CollideStrength:1,d3CollideIterations:1,d3GravityStrength:.1,d3GravityStrengthConnected:.001,enabled:!0,cooldownTime:2e3,useWorker:!0,warmupTicks:"auto",freezeNodesOnDrag:!0,gridSnappingEnabled:!1,gridSize:50,fitViewOnExpandCollapse:!1,layout:{type:"force"},callbacks:{onInit:()=>{},onStart:()=>{},onStop:()=>{},onTick:()=>{}}},ft={repulsion:[0,100],linkDistance:[40,260],collisionRadius:[4,60],friction:[0,100]},Ja={tight:{repulsion:32,linkDistance:70,collisionRadius:16,friction:58},loose:{repulsion:70,linkDistance:150,collisionRadius:26,friction:28},default:{repulsion:70,linkDistance:150,collisionRadius:26,friction:28}},O=class O{constructor(t,n={}){x(this,"simulation");x(this,"graph");x(this,"canvas");x(this,"graphInteraction");x(this,"layout");x(this,"canvasBCR");x(this,"animationFrameId",null);x(this,"startSimulationTime",0);x(this,"engineRunning",!1);x(this,"slowTickThresholdReached",!1);x(this,"avgTickDuration",0);x(this,"SLOW_TICK_THRESHOLD",33);x(this,"dragInProgress",!1);x(this,"dragSelection",[]);x(this,"totalTickCount",0);x(this,"options");x(this,"callbacks");x(this,"simulationForces");x(this,"scaledForces",{d3ManyBodyStrength:ee.d3ManyBodyStrength,d3CollideStrength:ee.d3CollideStrength});x(this,"physicsKnobs");if(this.graph=t,this.options=sn({},ee,n),this.callbacks=this.options.callbacks??{},this.physicsKnobs=O.knobsFromOptions(this.options),this.canvas=this.graph.renderer.getCanvas(),!this.canvas)throw new Error("Canvas element is not defined in the graph renderer.");if(this.canvasBCR=this.canvas.getBoundingClientRect(),this.graphInteraction=this.graph.renderer.getGraphInteraction(),!this.graphInteraction)throw new Error("Graph interaction is not available.");const i=O.initSimulationForces(this.options,this.canvasBCR);this.simulation=i.simulation,this.simulationForces=i.simulationForces,this.scaledForces.d3ManyBodyStrength=this.options.d3ManyBodyStrength||ee.d3ManyBodyStrength,this.scaledForces.d3CollideStrength=this.options.d3CollideStrength||ee.d3CollideStrength,this.options.layout.type==="tree"?this.layout=new it(this.graph,this.simulation,this.simulationForces,this.options.layout):this.options.layout.type==="egoTree"&&(this.layout=new Oe(this.graph,this.simulation,this.simulationForces,this.options.layout)),this.callbacks.onInit&&this.callbacks.onInit(this)}static initSimulationForces(t,n){const i={link:Cr(),charge:Yr(),collide:Ir(),gravity:Kr()},r=$r().force("link",i.link).force("charge",i.charge).force("collide",i.collide).force("gravity",i.gravity);return this.initSimulationForceGravity(i.gravity,t,n),this.initSimulationForceLink(i.link,t),this.initSimulationForceCharge(i.charge,t),this.initSimulationForceCollide(i.collide,t),r.alphaMin(t.d3AlphaMin),r.alphaDecay(t.d3AlphaDecay),r.alphaTarget(0),r.velocityDecay(t.d3VelocityDecay),{simulation:r,simulationForces:i}}static initSimulationForceGravity(t,n,i){t.x(i.width/2).y(i.height/2).strength(r=>(r.degree()??0)===0?n.d3GravityStrength:n.d3GravityStrengthConnected)}static initSimulationForceLink(t,n){t.distance(i=>{const r=i.__clusterAnchorDistance;if(r!=null)return r;const o=Qa(i);if(!o||o==="")return n.d3LinkDistance;const a=o.length*10;return Math.max(n.d3LinkDistance,a)}),n.d3LinkStrength&&t.strength(n.d3LinkStrength)}static initSimulationForceCharge(t,n){t.theta(n.d3ManyBodyTheta).strength(i=>{const r=i,o=n.d3ManyBodyStrength,a=r.expanded?r.getCircleRadiusCollapsed():r.getCircleRadius(),l=10+Math.sqrt(Math.max(0,a-10));let h=r.weight??1;return h*=r.isParent?10:1,o*(l*l)/100*h})}static initSimulationForceCollide(t,n){const i=n.d3CollideRadiusMultiplier;t.radius(r=>{const o=r;return o.expanded?i*o.getCircleRadius()+20:o.getCircleRadius()?i*o.getCircleRadius():n.d3CollideRadius}).strength(n.d3CollideStrength)}static initSimulationForceClusterRadialConstraint(t,n){t.strength(n.d3CollideStrength)}update(){this.layout&&this.layout.update();const t=this.graph.getMutableNodes().filter(i=>i.visible);this.simulation.nodes(t);const n=this.simulation.force("link");n&&n.id(i=>i.id).links(this.getActiveEdges()),this.restart()}getActiveEdges(){const t=new Set(this.graph.getMutableNodes().filter(a=>a.visible).map(a=>a.id)),n=a=>{let l=a;for(;l&&!t.has(l.id);)l=l.parentNode;return l},i=(a,l)=>a<l?`${a}|${l}`:`${l}|${a}`,r=[],o=new Set;for(const a of this.graph.getMutableEdges()){if(!a.visible)continue;const l=a.source,h=a.target;if(!l.isChild&&!h.isChild){r.push(a),o.add(i(l.id,h.id));continue}if(l.isChild&&h.isChild)continue;const s=l.isChild?h:l,f=n(l.isChild?l:h);if(!f||f.id===s.id)continue;const b=i(s.id,f.id);o.has(b)||(o.add(b),r.push(this.clusterAnchorLink(s,f)))}return r}clusterAnchorLink(t,n){return{id:`cluster-anchor-${t.id}-${n.id}`,source:t,target:n,__clusterAnchorDistance:n.getCircleRadius()+this.options.d3LinkDistance}}scaleSimulationOptions(){const t=O.scaleSimulationOptions(this.options,this.canvasBCR,this.graph.getNodeCount());this.scaledForces.d3ManyBodyStrength=t.d3ManyBodyStrength??ee.d3ManyBodyStrength,this.scaledForces.d3CollideStrength=t.d3CollideStrength??ee.d3CollideStrength}static scaleSimulationOptions(t,n,i){const r=i/(n.width*n.height),o=Math.min(2,75e-6/r);return{d3ManyBodyStrength:t.d3ManyBodyStrength*o,d3CollideStrength:t.d3ManyBodyStrength*o}}applyScalledSimulationOptions(){O.initSimulationForceCharge(this.simulationForces.charge,this.options),O.initSimulationForceCollide(this.simulationForces.collide,this.options)}enable(){this.avgTickDuration=0,this.options.enabled=!0,this.start(!1)}disable(){this.options.enabled=!1,this.stop()}pause(){this.engineRunning=!1,this.slowTickThresholdReached=!1}restart(){this.startSimulationTime=new Date().getTime(),this.engineRunning=!0,this.slowTickThresholdReached=!1}async start(t=!0){if(t&&await this.runSimulationWorkerRouter(),!this.options.enabled){this.engineRunning=!1;return}this.engineRunning=!0,this.slowTickThresholdReached=!1,this.callbacks.onStart&&this.callbacks.onStart(this),this.animationFrameId===null&&this.startAnimationLoop()}stop(){this.engineRunning=!1,this.animationFrameId!==null&&(cancelAnimationFrame(this.animationFrameId),this.animationFrameId=null),this.simulation.stop(),this.callbacks.onStop&&this.callbacks.onStop(this)}startAnimationLoop(){const t=()=>{this.animationFrameId=requestAnimationFrame(t),this.simulationTick()};this.engineRunning=!0,this.simulation.alpha(.01).restart(),this.animationFrameId=requestAnimationFrame(t)}simulationTick(){if(this.engineRunning){!this.dragInProgress&&(new Date().getTime()-this.startSimulationTime>this.options.cooldownTime||this.options.d3AlphaMin>0&&this.simulation.alpha()<this.options.d3AlphaMin)&&(this.engineRunning=!1,this.simulation.stop(),this.callbacks.onStop&&this.callbacks.onStop(this)),this.totalTickCount++;const t=performance.now();this.simulation.tick(),this.graph.nextTick(),this.updateTickMetrics(performance.now()-t),this.callbacks.onTick&&this.callbacks.onTick(this),this.graphInteraction.simulationTick(),this.totalTickCount%10===0&&this.graphInteraction.simulationSlowTick()}}updateTickMetrics(t){var n;this.avgTickDuration=this.avgTickDuration*.9+t*.1,this.avgTickDuration>this.SLOW_TICK_THRESHOLD&&(this.slowTickThresholdReached=!0,this.disable(),this.graph.UIManager.showNotification({level:"warning",title:"Physics engine running slow",message:"The physic has been disabled."}),(n=this.graph.UIManager.viewFlyout)==null||n.syncRunState())}async waitForSimulationStop(){if(this.engineRunning)return new Promise(t=>{const n=this.callbacks.onStop;this.callbacks.onStop=i=>{n==null||n(i),this.callbacks.onStop=n,t()}})}isEnabled(){return this.options.enabled}applyComputedPositions(t){const n=new Map(t.map(i=>[i.id,i]));for(const i of this.graph.getMutableNodes()){const r=n.get(i.id);r&&(i.x=r.x,i.y=r.y,i.fx=typeof r.fx=="number"?r.fx:void 0,i.fy=typeof r.fy=="number"?r.fy:void 0)}}async computeGraph(t={}){var f;const{runSimulation:n}=await Promise.resolve().then(function(){return el}),i=(f=this.canvas)==null?void 0:f.getBoundingClientRect();if(!i)return;const r=this.graph.getMutableNodes(),o=this.graph.getNodes(),a=this.graph.getEdges(),{callbacks:l,...h}=this.options;Object.assign(h,t);const{nodes:s}=n(o,a,h,i);this.applyComputedPositions(s),this.graph.updateData(r,void 0,!1)}async runSimulationWorkerRouter(t={}){if(this.options.useWorker)try{await this.runSimulationWorker(t);return}catch(n){this.options.useWorker=!1,console.warn("[Pivotick] Simulation Web Worker unavailable (often a CSP blocking blob workers); falling back to the main thread. Set `simulation.useWorker: false` to silence this.",n)}await this.computeGraph(t),this.graph.updateLayoutProgress(100,0,"done")}async runSimulationWorker(t={}){var f;const n=(f=this.canvas)==null?void 0:f.getBoundingClientRect();if(!n)return;const i=this.graph.getMutableNodes(),r=this.graph.getNodes().map(b=>b.toSimulationDTO()),o=this.graph.getEdges().map(b=>b.toSimulationDTO()),a=(b,m)=>{this.graph.updateLayoutProgress(b,m,"simulation")},{callbacks:l,...h}=this.options;Object.assign(h,t);const{nodes:s}=await Ta(r,o,h,n,a);this.graph.updateLayoutProgress(100,0,"rendering"),this.applyComputedPositions(s),this.graph.updateData(i,void 0,!1),this.graph.updateLayoutProgress(100,0,"done")}reheat(t=.7){this.restart(),this.simulation.alpha(t).restart()}refreshForcesAndReheat(t=.5){if(!this.options.enabled)return;const n=this.graph.getMutableNodes().filter(i=>i.visible);this.simulation.nodes(n),this.reheat(t)}setRepulsion(t){const n=O.clamp(t,ft.repulsion);this.physicsKnobs.repulsion=n,this.options.d3ManyBodyStrength=O.mapLinear(n,ft.repulsion,O.REPULSION_STRENGTH_RANGE),this.scaledForces.d3ManyBodyStrength=this.options.d3ManyBodyStrength,O.initSimulationForceCharge(this.simulationForces.charge,this.options),this.reheatIfEnabled()}setLinkDistance(t){const n=O.clamp(t,ft.linkDistance);this.physicsKnobs.linkDistance=n,this.options.d3LinkDistance=O.mapLinear(n,ft.linkDistance,O.LINK_DISTANCE_RANGE),O.initSimulationForceLink(this.simulationForces.link,this.options),this.reheatIfEnabled()}setCollisionRadius(t){const n=O.clamp(t,ft.collisionRadius);this.physicsKnobs.collisionRadius=n,this.options.d3CollideRadiusMultiplier=O.mapLinear(n,ft.collisionRadius,O.COLLIDE_MULTIPLIER_RANGE),O.initSimulationForceCollide(this.simulationForces.collide,this.options),this.reheatIfEnabled()}setFriction(t){const n=O.clamp(t,ft.friction);this.physicsKnobs.friction=n,this.options.d3VelocityDecay=O.mapLinear(n,ft.friction,O.FRICTION_DECAY_RANGE),this.simulation.velocityDecay(this.options.d3VelocityDecay)}applyPhysicsPreset(t){const n=Ja[t];this.physicsKnobs={...n},this.options.d3ManyBodyStrength=O.mapLinear(n.repulsion,ft.repulsion,O.REPULSION_STRENGTH_RANGE),this.scaledForces.d3ManyBodyStrength=this.options.d3ManyBodyStrength,this.options.d3LinkDistance=O.mapLinear(n.linkDistance,ft.linkDistance,O.LINK_DISTANCE_RANGE),this.options.d3CollideRadiusMultiplier=O.mapLinear(n.collisionRadius,ft.collisionRadius,O.COLLIDE_MULTIPLIER_RANGE),this.options.d3VelocityDecay=O.mapLinear(n.friction,ft.friction,O.FRICTION_DECAY_RANGE),O.initSimulationForceCharge(this.simulationForces.charge,this.options),O.initSimulationForceLink(this.simulationForces.link,this.options),O.initSimulationForceCollide(this.simulationForces.collide,this.options),this.simulation.velocityDecay(this.options.d3VelocityDecay),this.reheatIfEnabled()}getPhysicsKnobs(){return{...this.physicsKnobs}}getLayoutType(){return this.options.layout.type}reheatIfEnabled(t=.5){this.options.enabled&&this.reheat(t)}static clamp(t,[n,i]){return Math.max(n,Math.min(i,t))}static mapLinear(t,n,i){const r=(t-n[0])/(n[1]-n[0]);return i[0]+r*(i[1]-i[0])}static knobsFromOptions(t){const n=(i,r,o)=>Math.round(O.clamp(O.mapLinear(i,r,ft[o]),ft[o]));return{repulsion:n(t.d3ManyBodyStrength,O.REPULSION_STRENGTH_RANGE,"repulsion"),linkDistance:n(t.d3LinkDistance,O.LINK_DISTANCE_RANGE,"linkDistance"),collisionRadius:n(t.d3CollideRadiusMultiplier,O.COLLIDE_MULTIPLIER_RANGE,"collisionRadius"),friction:n(t.d3VelocityDecay,O.FRICTION_DECAY_RANGE,"friction")}}createDragBehavior(){return Us().filter(()=>!this.graph.editing.connectManager.isActiveAndNotIdle()).on("start.draggedelement",(t,n)=>{this.graphInteraction.hasActiveMultiselection()?this.dragSelection=this.graphInteraction.getSelectedNodes().map(i=>{const{node:r}=i;return r.freeze(),{node:r,dx:r.x-n.x,dy:r.y-n.y}}):(this.dragSelection=[],n.freeze())}).on("drag.draggedelement",(t,n)=>{if(!this.dragInProgress&&this.isEnabled()&&(this.dragInProgress=!0,this.restart(),this.simulation.alphaTarget(.3).restart()),this.graphInteraction.hasActiveMultiselection())this.dragSelection.forEach(({node:i,dx:r,dy:o})=>{const a=this.applySnap(t.x+r),l=this.applySnap(t.y+o);i.fx=a,i.fy=l,i.x=a,i.y=l});else{const i=this.applySnap(t.x),r=this.applySnap(t.y);n.fx=i,n.fy=r,n.x=i,n.y=r}if(this.graphInteraction.dragging(t.sourceEvent,t.subject),!this.engineRunning||!this.isEnabled()){const i=this.graphInteraction.hasActiveMultiselection()?this.dragSelection.map(r=>r.node):[n];this.graph.nextTickFor(i)}}).on("end.draggedelement",(t,n)=>{!t.active&&this.dragInProgress&&(this.dragInProgress=!1,this.restart(),this.simulation.alphaTarget(this.options.d3AlphaTarget).restart()),this.options.freezeNodesOnDrag||(this.graphInteraction.hasActiveMultiselection()?(this.dragSelection.forEach(({node:i})=>i.unfreeze()),this.dragSelection=[]):n.unfreeze()),this.graphInteraction.dragended(t.sourceEvent,t.subject)})}isDragging(){return this.dragInProgress}toggleGridSnapping(){this.options.gridSnappingEnabled=!this.options.gridSnappingEnabled}toggleFreezeNodesOnDrag(){this.options.freezeNodesOnDrag=!this.options.freezeNodesOnDrag}isFreezeNodesOnDrag(){return this.options.freezeNodesOnDrag}isGridSnappingEnabled(){return this.options.gridSnappingEnabled}toggleFitViewOnExpandCollapse(){this.options.fitViewOnExpandCollapse=!this.options.fitViewOnExpandCollapse}isFitViewOnExpandCollapse(){return this.options.fitViewOnExpandCollapse}applySnap(t){return this.options.gridSnappingEnabled?Math.round(t/this.options.gridSize)*this.options.gridSize:t}snapToGrid(t){return this.applySnap(t)}getForceSimulation(){return this.simulationForces}getSimulation(){return this.simulation}async changeLayout(t,n={}){var i;this.layout&&((i=this.layout)==null||i.unregisterLayout(),this.layout=void 0),n=n??{},n.layout=n.layout??{},n.layout.type=t,t==="force"?this.applyScalledSimulationOptions():t==="tree"&&(this.layout=new it(this.graph,this.simulation,this.simulationForces,n.layout)),this.options.layout.type=t,this.update(),this.pause(),await this.runSimulationWorkerRouter(n),this.restart(),await this.waitForSimulationStop(),this.graph.renderer.fitAndCenterWhenSettled()}};x(O,"REPULSION_STRENGTH_RANGE",[0,-400]),x(O,"LINK_DISTANCE_RANGE",[40,260]),x(O,"COLLIDE_MULTIPLIER_RANGE",[.6,2.4]),x(O,"FRICTION_DECAY_RANGE",[0,1]);let un=O;const nr=1e4,hn=2e4,fn=.15*hn;self.onmessage=e=>{var y,_,T,v;if(e.data.source!=="simulation-worker-wrapper")return;const{nodes:t,edges:n,options:i,canvasBCR:r}=e.data,o=t.map(E=>{const g=new Qi(E.id,E.data,E.style);return g.setCircleRadius(E._circleRadius??10),typeof E.x=="number"&&(g.x=E.x),typeof E.y=="number"&&(g.y=E.y),typeof E.fx=="number"&&(g.fx=E.fx),typeof E.fy=="number"&&(g.fy=E.fy),g}),a=new Map(o.map(E=>[E.id,E]));(y=i.layout)==null||y.type;const{simulation:l,simulationForces:h}=un.initSimulationForces(i,r),s=[];for(const E of n){const g=a.get(E.from.id),R=a.get(E.to.id);if(g&&R){const C=E.style??{};s.push(new rn(E.id,g,R,E.data,C,E.directed))}}l.nodes(o);const f=l.force("link");f&&f.id(E=>E.id).links(s),((_=i.layout)==null?void 0:_.type)==="tree"?it.registerForcesOnSimulation(o,s,l,h,i.layout,r,it):((T=i.layout)==null?void 0:T.type)==="egoTree"&&it.registerForcesOnSimulation(o,s,l,h,i.layout,r,Oe);let b=i.warmupTicks||hn;b=b==="auto"?hn:b,b=b-fn;let m=.3;l.alphaTarget(m);const p=new Date().getTime();let A;for(let E=0;E<b&&!(new Date().getTime()-p>nr||new Date().getTime()-p>i.cooldownTime||dn(i,l,m)&&new Date().getTime()-p>i.cooldownTime*.15);++E)E%5===0&&(A=ir(E,new Date().getTime()-p,i),postMessage({type:"tick",progress:A,elapsedTime:new Date().getTime()-p})),l.tick();m=0,l.alphaTarget(m),l.alpha(1);for(let E=0;E<fn&&!(dn(i,l,m)&&new Date().getTime()-p>i.cooldownTime*.15);++E)l.tick(),E%5===0&&(A=ir(b+E,new Date().getTime()-p,i),postMessage({type:"tick",progress:A,elapsedTime:new Date().getTime()-p}));postMessage({type:"tick",progress:1,elapsedTime:new Date().getTime()-p}),((v=i.layout)==null?void 0:v.type)==="tree"&&it.simulationDone(o,s,l,i.layout),postMessage({type:"done",nodes:o.map(E=>E.toDict()),edges:s.map(E=>E.toDict())})};function tl(e,t,n,i){var p,A,y,_;const r=e.map(T=>{const v=new Qi(T.id,T.getData(),T.getStyle());return v.weight=T.weight||1,v.setCircleRadius(T.getCircleRadius()),typeof T.x=="number"&&(v.x=T.x),typeof T.y=="number"&&(v.y=T.y),typeof T.fx=="number"&&(v.fx=T.fx),typeof T.fy=="number"&&(v.fy=T.fy),v}),o=new Map(r.map(T=>[T.id,T]));(p=n.layout)==null||p.type;const{simulation:a,simulationForces:l}=un.initSimulationForces(n,i),h=[];for(const T of t){const v=o.get(T.from.id),E=o.get(T.to.id);if(v&&E){const g=T.getStyle()??{};h.push(new rn(T.id,v,E,T.getData(),g,T.directed))}}a.nodes(r);const s=a.force("link");s&&s.id(T=>T.id).links(h),(((A=n.layout)==null?void 0:A.type)==="tree"||((y=n.layout)==null?void 0:y.type)==="egoTree")&&it.registerForcesOnSimulation(r,h,a,l,n.layout,i,it);let f;n.warmupTicks==="auto"||n.warmupTicks==null?f=hn:f=n.warmupTicks,f=f-fn;let b=.3;a.alphaTarget(b);const m=new Date().getTime();for(let T=0;T<f&&!(new Date().getTime()-m>nr||new Date().getTime()-m>n.cooldownTime||dn(n,a,b)&&new Date().getTime()-m>n.cooldownTime*.15);++T)a.tick();b=0,a.alphaTarget(b),a.alpha(1);for(let T=0;T<fn&&!(dn(n,a,b)&&new Date().getTime()-m>n.cooldownTime*.15);++T)a.tick();return((_=n.layout)==null?void 0:_.type)==="tree"&&it.simulationDone(r,h,a,n.layout),{nodes:r,edges:h}}function ir(e,t,n){return t/n.cooldownTime}function dn(e,t,n){return e.d3AlphaMin>0&&t.alpha()-n<e.d3AlphaMin}var el=Object.freeze({__proto__:null,runSimulation:tl})})();\n', zi = typeof self < "u" && self.Blob && new Blob([ur], { type: "text/javascript;charset=utf-8" });
function Ua(e) {
  let t;
  try {
    if (t = zi && (self.URL || self.webkitURL).createObjectURL(zi), !t) throw "";
    const n = new Worker(t, {
      name: e == null ? void 0 : e.name
    });
    return n.addEventListener("error", () => {
      (self.URL || self.webkitURL).revokeObjectURL(t);
    }), n;
  } catch {
    return new Worker(
      "data:text/javascript;charset=utf-8," + encodeURIComponent(ur),
      {
        name: e == null ? void 0 : e.name
      }
    );
  } finally {
    t && (self.URL || self.webkitURL).revokeObjectURL(t);
  }
}
function ja() {
  return new Ua();
}
const Ga = (e, t, n, i, r) => new Promise((o, a) => {
  const l = ja();
  l.postMessage({ source: "simulation-worker-wrapper", nodes: e, edges: t, options: n, canvasBCR: i }), l.onmessage = (h) => {
    const { type: s, progress: f, nodes: w, edges: m, elapsedTime: p } = h.data;
    if (s === "tick" && typeof f == "number") {
      r == null || r(f, p);
      return;
    }
    s === "done" && (o({ nodes: w, edges: m }), l.terminate());
  }, l.onerror = a;
});
var hn = typeof globalThis < "u" ? globalThis : typeof window < "u" ? window : typeof global < "u" ? global : typeof self < "u" ? self : {};
function Ha(e) {
  return e && e.__esModule && Object.prototype.hasOwnProperty.call(e, "default") ? e.default : e;
}
var je = { exports: {} };
je.exports;
var Bi;
function Wa() {
  return Bi || (Bi = 1, (function(e, t) {
    var n = 200, i = "__lodash_hash_undefined__", r = 800, o = 16, a = 9007199254740991, l = "[object Arguments]", h = "[object Array]", s = "[object AsyncFunction]", f = "[object Boolean]", w = "[object Date]", m = "[object Error]", p = "[object Function]", A = "[object GeneratorFunction]", y = "[object Map]", _ = "[object Number]", v = "[object Null]", S = "[object Object]", x = "[object Proxy]", g = "[object RegExp]", I = "[object Set]", C = "[object String]", L = "[object Undefined]", F = "[object WeakMap]", Y = "[object ArrayBuffer]", j = "[object DataView]", Q = "[object Float32Array]", at = "[object Float64Array]", ot = "[object Int8Array]", q = "[object Int16Array]", Ht = "[object Int32Array]", Nt = "[object Uint8Array]", kt = "[object Uint8ClampedArray]", ve = "[object Uint16Array]", se = "[object Uint32Array]", Qt = /[\\^$.*+?()[\]{}|]/g, Ye = /^\[object .+?Constructor\]$/, Nn = /^(?:0|[1-9]\d*)$/, G = {};
    G[Q] = G[at] = G[ot] = G[q] = G[Ht] = G[Nt] = G[kt] = G[ve] = G[se] = !0, G[l] = G[h] = G[Y] = G[f] = G[j] = G[w] = G[m] = G[p] = G[y] = G[_] = G[S] = G[g] = G[I] = G[C] = G[F] = !1;
    var K = typeof hn == "object" && hn && hn.Object === Object && hn, be = typeof self == "object" && self && self.Object === Object && self, $ = K || be || Function("return this")(), ae = t && !t.nodeType && t, H = ae && !0 && e && !e.nodeType && e, Wt = H && H.exports === ae, Vt = Wt && K.process, St = (function() {
      try {
        var c = H && H.require && H.require("util").types;
        return c || Vt && Vt.binding && Vt.binding("util");
      } catch {
      }
    })(), we = St && St.isTypedArray;
    function Te(c, d, b) {
      switch (b.length) {
        case 0:
          return c.call(d);
        case 1:
          return c.call(d, b[0]);
        case 2:
          return c.call(d, b[0], b[1]);
        case 3:
          return c.call(d, b[0], b[1], b[2]);
      }
      return c.apply(d, b);
    }
    function $e(c, d) {
      for (var b = -1, R = Array(c); ++b < c; )
        R[b] = d(b);
      return R;
    }
    function qe(c) {
      return function(d) {
        return c(d);
      };
    }
    function Dt(c, d) {
      return c == null ? void 0 : c[d];
    }
    function Jt(c, d) {
      return function(b) {
        return c(d(b));
      };
    }
    var Lt = Array.prototype, Se = Function.prototype, Yt = Object.prototype, Rt = $["__core-js_shared__"], Ft = Se.toString, pt = Yt.hasOwnProperty, Ae = (function() {
      var c = /[^.]+$/.exec(Rt && Rt.keys && Rt.keys.IE_PROTO || "");
      return c ? "Symbol(src)_1." + c : "";
    })(), xe = Yt.toString, Ke = Ft.call(Object), Ee = RegExp(
      "^" + Ft.call(pt).replace(Qt, "\\$&").replace(/hasOwnProperty|(function).*?(?=\\\()| for .+?(?=\\\])/g, "$1.*?") + "$"
    ), It = Wt ? $.Buffer : void 0, Pt = $.Symbol, gt = $.Uint8Array;
    It && It.allocUnsafe;
    var le = Jt(Object.getPrototypeOf, Object), Ne = Object.create, Xe = Yt.propertyIsEnumerable, De = Lt.splice, Ct = Pt ? Pt.toStringTag : void 0, zt = (function() {
      try {
        var c = kn(Object, "defineProperty");
        return c({}, "", {}), c;
      } catch {
      }
    })(), ce = It ? It.isBuffer : void 0, mt = Math.max, $t = Date.now, ue = kn($, "Map"), Bt = kn(Object, "create"), Dn = /* @__PURE__ */ (function() {
      function c() {
      }
      return function(d) {
        if (!ie(d))
          return {};
        if (Ne)
          return Ne(d);
        c.prototype = d;
        var b = new c();
        return c.prototype = void 0, b;
      };
    })();
    function At(c) {
      var d = -1, b = c == null ? 0 : c.length;
      for (this.clear(); ++d < b; ) {
        var R = c[d];
        this.set(R[0], R[1]);
      }
    }
    function Re() {
      this.__data__ = Bt ? Bt(null) : {}, this.size = 0;
    }
    function Rn(c) {
      var d = this.has(c) && delete this.__data__[c];
      return this.size -= d ? 1 : 0, d;
    }
    function te(c) {
      var d = this.__data__;
      if (Bt) {
        var b = d[c];
        return b === i ? void 0 : b;
      }
      return pt.call(d, c) ? d[c] : void 0;
    }
    function In(c) {
      var d = this.__data__;
      return Bt ? d[c] !== void 0 : pt.call(d, c);
    }
    function Cn(c, d) {
      var b = this.__data__;
      return this.size += this.has(c) ? 0 : 1, b[c] = Bt && d === void 0 ? i : d, this;
    }
    At.prototype.clear = Re, At.prototype.delete = Rn, At.prototype.get = te, At.prototype.has = In, At.prototype.set = Cn;
    function B(c) {
      var d = -1, b = c == null ? 0 : c.length;
      for (this.clear(); ++d < b; ) {
        var R = c[d];
        this.set(R[0], R[1]);
      }
    }
    function qt() {
      this.__data__ = [], this.size = 0;
    }
    function On(c) {
      var d = this.__data__, b = ne(d, c);
      if (b < 0)
        return !1;
      var R = d.length - 1;
      return b == R ? d.pop() : De.call(d, b, 1), --this.size, !0;
    }
    function Ze(c) {
      var d = this.__data__, b = ne(d, c);
      return b < 0 ? void 0 : d[b][1];
    }
    function Ie(c) {
      return ne(this.__data__, c) > -1;
    }
    function Qe(c, d) {
      var b = this.__data__, R = ne(b, c);
      return R < 0 ? (++this.size, b.push([c, d])) : b[R][1] = d, this;
    }
    B.prototype.clear = qt, B.prototype.delete = On, B.prototype.get = Ze, B.prototype.has = Ie, B.prototype.set = Qe;
    function Ut(c) {
      var d = -1, b = c == null ? 0 : c.length;
      for (this.clear(); ++d < b; ) {
        var R = c[d];
        this.set(R[0], R[1]);
      }
    }
    function Mn() {
      this.size = 0, this.__data__ = {
        hash: new At(),
        map: new (ue || B)(),
        string: new At()
      };
    }
    function yt(c) {
      var d = an(this, c).delete(c);
      return this.size -= d ? 1 : 0, d;
    }
    function jt(c) {
      return an(this, c).get(c);
    }
    function Je(c) {
      return an(this, c).has(c);
    }
    function tn(c, d) {
      var b = an(this, c), R = b.size;
      return b.set(c, d), this.size += b.size == R ? 0 : 1, this;
    }
    Ut.prototype.clear = Mn, Ut.prototype.delete = yt, Ut.prototype.get = jt, Ut.prototype.has = Je, Ut.prototype.set = tn;
    function Gt(c) {
      var d = this.__data__ = new B(c);
      this.size = d.size;
    }
    function he() {
      this.__data__ = new B(), this.size = 0;
    }
    function ee(c) {
      var d = this.__data__, b = d.delete(c);
      return this.size = d.size, b;
    }
    function fe(c) {
      return this.__data__.get(c);
    }
    function xt(c) {
      return this.__data__.has(c);
    }
    function en(c, d) {
      var b = this.__data__;
      if (b instanceof B) {
        var R = b.__data__;
        if (!ue || R.length < n - 1)
          return R.push([c, d]), this.size = ++b.size, this;
        b = this.__data__ = new Ut(R);
      }
      return b.set(c, d), this.size = b.size, this;
    }
    Gt.prototype.clear = he, Gt.prototype.delete = ee, Gt.prototype.get = fe, Gt.prototype.has = xt, Gt.prototype.set = en;
    function nn(c, d) {
      var b = Pn(c), R = !b && Fn(c), P = !b && !R && ci(c), V = !b && !R && !P && hi(c), X = b || R || P || V, U = X ? $e(c.length, String) : [], Z = U.length;
      for (var Et in c)
        X && // Safari 9 has enumerable `arguments.length` in strict mode.
        (Et == "length" || // Node.js 0.10 has enumerable non-index properties on buffers.
        P && (Et == "offset" || Et == "parent") || // PhantomJS 2 has enumerable non-index properties on typed arrays.
        V && (Et == "buffer" || Et == "byteLength" || Et == "byteOffset") || // Skip index properties.
        ai(Et, Z)) || U.push(Et);
      return U;
    }
    function Ce(c, d, b) {
      (b !== void 0 && !ln(c[d], b) || b === void 0 && !(d in c)) && Kt(c, d, b);
    }
    function rn(c, d, b) {
      var R = c[d];
      (!(pt.call(c, d) && ln(R, b)) || b === void 0 && !(d in c)) && Kt(c, d, b);
    }
    function ne(c, d) {
      for (var b = c.length; b--; )
        if (ln(c[b][0], d))
          return b;
      return -1;
    }
    function Kt(c, d, b) {
      d == "__proto__" && zt ? zt(c, d, {
        configurable: !0,
        enumerable: !0,
        value: b,
        writable: !0
      }) : c[d] = b;
    }
    var Xt = pr();
    function D(c) {
      return c == null ? c === void 0 ? L : v : Ct && Ct in Object(c) ? gr(c) : wr(c);
    }
    function u(c) {
      return Me(c) && D(c) == l;
    }
    function T(c) {
      if (!ie(c) || vr(c))
        return !1;
      var d = Bn(c) ? Ee : Ye;
      return d.test(xr(c));
    }
    function N(c) {
      return Me(c) && ui(c.length) && !!G[D(c)];
    }
    function z(c) {
      if (!ie(c))
        return br(c);
      var d = li(c), b = [];
      for (var R in c)
        R == "constructor" && (d || !pt.call(c, R)) || b.push(R);
      return b;
    }
    function k(c, d, b, R, P) {
      c !== d && Xt(d, function(V, X) {
        if (P || (P = new Gt()), ie(V))
          W(c, d, X, b, k, R, P);
        else {
          var U = R ? R(Ln(c, X), V, X + "", c, d, P) : void 0;
          U === void 0 && (U = V), Ce(c, X, U);
        }
      }, fi);
    }
    function W(c, d, b, R, P, V, X) {
      var U = Ln(c, b), Z = Ln(d, b), Et = X.get(Z);
      if (Et) {
        Ce(c, b, Et);
        return;
      }
      var vt = V ? V(U, Z, b + "", c, d, X) : void 0, ke = vt === void 0;
      if (ke) {
        var Un = Pn(Z), jn = !Un && ci(Z), pi = !Un && !jn && hi(Z);
        vt = Z, Un || jn || pi ? Pn(U) ? vt = U : Er(U) ? vt = on(U) : jn ? (ke = !1, vt = _t(Z)) : pi ? (ke = !1, vt = tt(Z)) : vt = [] : Nr(Z) || Fn(Z) ? (vt = U, Fn(U) ? vt = Dr(U) : (!ie(U) || Bn(U)) && (vt = mr(Z))) : ke = !1;
      }
      ke && (X.set(Z, vt), P(vt, Z, R, V, X), X.delete(Z)), Ce(c, b, vt);
    }
    function ft(c, d) {
      return Sr(Tr(c, d, di), c + "");
    }
    var bt = zt ? function(c, d) {
      return zt(c, "toString", {
        configurable: !0,
        enumerable: !1,
        value: Ir(d),
        writable: !0
      });
    } : di;
    function _t(c, d) {
      return c.slice();
    }
    function Oe(c) {
      var d = new c.constructor(c.byteLength);
      return new gt(d).set(new gt(c)), d;
    }
    function tt(c, d) {
      var b = Oe(c.buffer);
      return new c.constructor(b, c.byteOffset, c.length);
    }
    function on(c, d) {
      var b = -1, R = c.length;
      for (d || (d = Array(R)); ++b < R; )
        d[b] = c[b];
      return d;
    }
    function sn(c, d, b, R) {
      var P = !b;
      b || (b = {});
      for (var V = -1, X = d.length; ++V < X; ) {
        var U = d[V], Z = void 0;
        Z === void 0 && (Z = c[U]), P ? Kt(b, U, Z) : rn(b, U, Z);
      }
      return b;
    }
    function dr(c) {
      return ft(function(d, b) {
        var R = -1, P = b.length, V = P > 1 ? b[P - 1] : void 0, X = P > 2 ? b[2] : void 0;
        for (V = c.length > 3 && typeof V == "function" ? (P--, V) : void 0, X && yr(b[0], b[1], X) && (V = P < 3 ? void 0 : V, P = 1), d = Object(d); ++R < P; ) {
          var U = b[R];
          U && c(d, U, R, V);
        }
        return d;
      });
    }
    function pr(c) {
      return function(d, b, R) {
        for (var P = -1, V = Object(d), X = R(d), U = X.length; U--; ) {
          var Z = X[++P];
          if (b(V[Z], Z, V) === !1)
            break;
        }
        return d;
      };
    }
    function an(c, d) {
      var b = c.__data__;
      return _r(d) ? b[typeof d == "string" ? "string" : "hash"] : b.map;
    }
    function kn(c, d) {
      var b = Dt(c, d);
      return T(b) ? b : void 0;
    }
    function gr(c) {
      var d = pt.call(c, Ct), b = c[Ct];
      try {
        c[Ct] = void 0;
        var R = !0;
      } catch {
      }
      var P = xe.call(c);
      return R && (d ? c[Ct] = b : delete c[Ct]), P;
    }
    function mr(c) {
      return typeof c.constructor == "function" && !li(c) ? Dn(le(c)) : {};
    }
    function ai(c, d) {
      var b = typeof c;
      return d = d ?? a, !!d && (b == "number" || b != "symbol" && Nn.test(c)) && c > -1 && c % 1 == 0 && c < d;
    }
    function yr(c, d, b) {
      if (!ie(b))
        return !1;
      var R = typeof d;
      return (R == "number" ? zn(b) && ai(d, b.length) : R == "string" && d in b) ? ln(b[d], c) : !1;
    }
    function _r(c) {
      var d = typeof c;
      return d == "string" || d == "number" || d == "symbol" || d == "boolean" ? c !== "__proto__" : c === null;
    }
    function vr(c) {
      return !!Ae && Ae in c;
    }
    function li(c) {
      var d = c && c.constructor, b = typeof d == "function" && d.prototype || Yt;
      return c === b;
    }
    function br(c) {
      var d = [];
      if (c != null)
        for (var b in Object(c))
          d.push(b);
      return d;
    }
    function wr(c) {
      return xe.call(c);
    }
    function Tr(c, d, b) {
      return d = mt(d === void 0 ? c.length - 1 : d, 0), function() {
        for (var R = arguments, P = -1, V = mt(R.length - d, 0), X = Array(V); ++P < V; )
          X[P] = R[d + P];
        P = -1;
        for (var U = Array(d + 1); ++P < d; )
          U[P] = R[P];
        return U[d] = b(X), Te(c, this, U);
      };
    }
    function Ln(c, d) {
      if (!(d === "constructor" && typeof c[d] == "function") && d != "__proto__")
        return c[d];
    }
    var Sr = Ar(bt);
    function Ar(c) {
      var d = 0, b = 0;
      return function() {
        var R = $t(), P = o - (R - b);
        if (b = R, P > 0) {
          if (++d >= r)
            return arguments[0];
        } else
          d = 0;
        return c.apply(void 0, arguments);
      };
    }
    function xr(c) {
      if (c != null) {
        try {
          return Ft.call(c);
        } catch {
        }
        try {
          return c + "";
        } catch {
        }
      }
      return "";
    }
    function ln(c, d) {
      return c === d || c !== c && d !== d;
    }
    var Fn = u(/* @__PURE__ */ (function() {
      return arguments;
    })()) ? u : function(c) {
      return Me(c) && pt.call(c, "callee") && !Xe.call(c, "callee");
    }, Pn = Array.isArray;
    function zn(c) {
      return c != null && ui(c.length) && !Bn(c);
    }
    function Er(c) {
      return Me(c) && zn(c);
    }
    var ci = ce || Cr;
    function Bn(c) {
      if (!ie(c))
        return !1;
      var d = D(c);
      return d == p || d == A || d == s || d == x;
    }
    function ui(c) {
      return typeof c == "number" && c > -1 && c % 1 == 0 && c <= a;
    }
    function ie(c) {
      var d = typeof c;
      return c != null && (d == "object" || d == "function");
    }
    function Me(c) {
      return c != null && typeof c == "object";
    }
    function Nr(c) {
      if (!Me(c) || D(c) != S)
        return !1;
      var d = le(c);
      if (d === null)
        return !0;
      var b = pt.call(d, "constructor") && d.constructor;
      return typeof b == "function" && b instanceof b && Ft.call(b) == Ke;
    }
    var hi = we ? qe(we) : N;
    function Dr(c) {
      return sn(c, fi(c));
    }
    function fi(c) {
      return zn(c) ? nn(c) : z(c);
    }
    var Rr = dr(function(c, d, b) {
      k(c, d, b);
    });
    function Ir(c) {
      return function() {
        return c;
      };
    }
    function di(c) {
      return c;
    }
    function Cr() {
      return !1;
    }
    e.exports = Rr;
  })(je, je.exports)), je.exports;
}
var Va = Wa();
const dn = /* @__PURE__ */ Ha(Va);
function Ya(e) {
  var t = 0, n = e.children, i = n && n.length;
  if (!i) t = 1;
  else for (; --i >= 0; ) t += n[i].value;
  e.value = t;
}
function $a() {
  return this.eachAfter(Ya);
}
function qa(e, t) {
  let n = -1;
  for (const i of this)
    e.call(t, i, ++n, this);
  return this;
}
function Ka(e, t) {
  for (var n = this, i = [n], r, o, a = -1; n = i.pop(); )
    if (e.call(t, n, ++a, this), r = n.children)
      for (o = r.length - 1; o >= 0; --o)
        i.push(r[o]);
  return this;
}
function Xa(e, t) {
  for (var n = this, i = [n], r = [], o, a, l, h = -1; n = i.pop(); )
    if (r.push(n), o = n.children)
      for (a = 0, l = o.length; a < l; ++a)
        i.push(o[a]);
  for (; n = r.pop(); )
    e.call(t, n, ++h, this);
  return this;
}
function Za(e, t) {
  let n = -1;
  for (const i of this)
    if (e.call(t, i, ++n, this))
      return i;
}
function Qa(e) {
  return this.eachAfter(function(t) {
    for (var n = +e(t.data) || 0, i = t.children, r = i && i.length; --r >= 0; ) n += i[r].value;
    t.value = n;
  });
}
function Ja(e) {
  return this.eachBefore(function(t) {
    t.children && t.children.sort(e);
  });
}
function tl(e) {
  for (var t = this, n = el(t, e), i = [t]; t !== n; )
    t = t.parent, i.push(t);
  for (var r = i.length; e !== n; )
    i.splice(r, 0, e), e = e.parent;
  return i;
}
function el(e, t) {
  if (e === t) return e;
  var n = e.ancestors(), i = t.ancestors(), r = null;
  for (e = n.pop(), t = i.pop(); e === t; )
    r = e, e = n.pop(), t = i.pop();
  return r;
}
function nl() {
  for (var e = this, t = [e]; e = e.parent; )
    t.push(e);
  return t;
}
function il() {
  return Array.from(this);
}
function rl() {
  var e = [];
  return this.eachBefore(function(t) {
    t.children || e.push(t);
  }), e;
}
function ol() {
  var e = this, t = [];
  return e.each(function(n) {
    n !== e && t.push({ source: n.parent, target: n });
  }), t;
}
function* sl() {
  var e = this, t, n = [e], i, r, o;
  do
    for (t = n.reverse(), n = []; e = t.pop(); )
      if (yield e, i = e.children)
        for (r = 0, o = i.length; r < o; ++r)
          n.push(i[r]);
  while (n.length);
}
function En(e, t) {
  e instanceof Map ? (e = [void 0, e], t === void 0 && (t = cl)) : t === void 0 && (t = ll);
  for (var n = new We(e), i, r = [n], o, a, l, h; i = r.pop(); )
    if ((a = t(i.data)) && (h = (a = Array.from(a)).length))
      for (i.children = a, l = h - 1; l >= 0; --l)
        r.push(o = a[l] = new We(a[l])), o.parent = i, o.depth = i.depth + 1;
  return n.eachBefore(hl);
}
function al() {
  return En(this).eachBefore(ul);
}
function ll(e) {
  return e.children;
}
function cl(e) {
  return Array.isArray(e) ? e[1] : null;
}
function ul(e) {
  e.data.value !== void 0 && (e.value = e.data.value), e.data = e.data.data;
}
function hl(e) {
  var t = 0;
  do
    e.height = t;
  while ((e = e.parent) && e.height < ++t);
}
function We(e) {
  this.data = e, this.depth = this.height = 0, this.parent = null;
}
We.prototype = En.prototype = {
  constructor: We,
  count: $a,
  each: qa,
  eachAfter: Xa,
  eachBefore: Ka,
  find: Za,
  sum: Qa,
  sort: Ja,
  path: tl,
  ancestors: nl,
  descendants: il,
  leaves: rl,
  links: ol,
  copy: al,
  [Symbol.iterator]: sl
};
function fl(e, t) {
  return e.parent === t.parent ? 1 : 2;
}
function qn(e) {
  var t = e.children;
  return t ? t[0] : e.t;
}
function Kn(e) {
  var t = e.children;
  return t ? t[t.length - 1] : e.t;
}
function dl(e, t, n) {
  var i = n / (t.i - e.i);
  t.c -= i, t.s += n, e.c += i, t.z += n, t.m += n;
}
function pl(e) {
  for (var t = 0, n = 0, i = e.children, r = i.length, o; --r >= 0; )
    o = i[r], o.z += t, o.m += t, t += o.s + (n += o.c);
}
function gl(e, t, n) {
  return e.a.parent === t.parent ? e.a : n;
}
function pn(e, t) {
  this._ = e, this.parent = null, this.children = null, this.A = null, this.a = this, this.z = 0, this.m = 0, this.c = 0, this.s = 0, this.t = null, this.i = t;
}
pn.prototype = Object.create(We.prototype);
function ml(e) {
  for (var t = new pn(e, 0), n, i = [t], r, o, a, l; n = i.pop(); )
    if (o = n._.children)
      for (n.children = new Array(l = o.length), a = l - 1; a >= 0; --a)
        i.push(r = n.children[a] = new pn(o[a], a)), r.parent = n;
  return (t.parent = new pn(null, 0)).children = [t], t;
}
function hr() {
  var e = fl, t = 1, n = 1, i = null;
  function r(s) {
    var f = ml(s);
    if (f.eachAfter(o), f.parent.m = -f.z, f.eachBefore(a), i) s.eachBefore(h);
    else {
      var w = s, m = s, p = s;
      s.eachBefore(function(S) {
        S.x < w.x && (w = S), S.x > m.x && (m = S), S.depth > p.depth && (p = S);
      });
      var A = w === m ? 1 : e(w, m) / 2, y = A - w.x, _ = t / (m.x + A + y), v = n / (p.depth || 1);
      s.eachBefore(function(S) {
        S.x = (S.x + y) * _, S.y = S.depth * v;
      });
    }
    return s;
  }
  function o(s) {
    var f = s.children, w = s.parent.children, m = s.i ? w[s.i - 1] : null;
    if (f) {
      pl(s);
      var p = (f[0].z + f[f.length - 1].z) / 2;
      m ? (s.z = m.z + e(s._, m._), s.m = s.z - p) : s.z = p;
    } else m && (s.z = m.z + e(s._, m._));
    s.parent.A = l(s, m, s.parent.A || w[0]);
  }
  function a(s) {
    s._.x = s.z + s.parent.m, s.m += s.parent.m;
  }
  function l(s, f, w) {
    if (f) {
      for (var m = s, p = s, A = f, y = m.parent.children[0], _ = m.m, v = p.m, S = A.m, x = y.m, g; A = Kn(A), m = qn(m), A && m; )
        y = qn(y), p = Kn(p), p.a = s, g = A.z + S - m.z - _ + e(A._, m._), g > 0 && (dl(gl(A, s, w), s, g), _ += g, v += g), S += A.m, _ += m.m, x += y.m, v += p.m;
      A && !Kn(p) && (p.t = A, p.m += S - v), m && !qn(y) && (y.t = m, y.m += _ - x, w = s);
    }
    return w;
  }
  function h(s) {
    s.x *= t, s.y = s.depth * n;
  }
  return r.separation = function(s) {
    return arguments.length ? (e = s, r) : e;
  }, r.size = function(s) {
    return arguments.length ? (i = !1, t = +s[0], n = +s[1], r) : i ? null : [t, n];
  }, r.nodeSize = function(s) {
    return arguments.length ? (i = !0, t = +s[0], n = +s[1], r) : i ? [t, n] : null;
  }, r;
}
function gn(e, t) {
  const n = /* @__PURE__ */ new Map();
  for (const a of e)
    n.set(a.id, []);
  for (const { source: a, target: l } of t) {
    const h = n.get(a.id);
    h ? h.push(l.id) : n.set(a.id, [l.id]);
  }
  const i = /* @__PURE__ */ new Set(), r = /* @__PURE__ */ new Set(), o = [];
  for (const a of e)
    if (!i.has(a.id))
      for (i.add(a.id), r.add(a.id), o.push({ id: a.id, next: 0 }); o.length > 0; ) {
        const l = o[o.length - 1], h = n.get(l.id) ?? [];
        if (l.next >= h.length) {
          r.delete(l.id), o.pop();
          continue;
        }
        const s = h[l.next++];
        if (r.has(s)) return !0;
        i.has(s) || (i.add(s), r.add(s), o.push({ id: s, next: 0 }));
      }
  return !1;
}
function Ui(e, t) {
  const n = new Set(t.map((i) => i.target.id));
  for (const i of e)
    if (!n.has(i.id)) return i;
  return e[0];
}
const yl = 1e6;
function _l(e, t) {
  var l;
  const n = /* @__PURE__ */ new Map();
  for (const h of e)
    n.set(h.id, []);
  for (const h of t)
    (l = n.get(h.from.id)) == null || l.push(h.to);
  let i = 0, r = !1, o = null, a = -1;
  for (const h of e) {
    const s = /* @__PURE__ */ new Set([h.id]), f = [h];
    for (; f.length > 0 && !r; ) {
      const m = f.pop();
      for (const p of n.get(m.id) ?? []) {
        if (++i > yl) {
          r = !0;
          break;
        }
        s.has(p.id) || (s.add(p.id), f.push(p));
      }
    }
    const w = s.size - 1;
    if (w > a && (a = w, o = h), r) break;
  }
  return r && console.warn("Pivotick: reachability search hit its traversal cap, using the best root found so far."), o ?? e[0];
}
function vl(e, t) {
  const n = /* @__PURE__ */ new Map(), i = /* @__PURE__ */ new Map();
  for (const s of e)
    n.set(s.id, []), i.set(s.id, 0);
  for (const s of t)
    s.directed !== !1 && (n.get(s.from.id).push(s.to), i.set(s.to.id, (i.get(s.to.id) || 0) + 1));
  const r = [], o = e.filter((s) => i.get(s.id) === 0);
  for (; o.length; ) {
    const s = o.shift();
    r.push(s);
    for (const f of n.get(s.id))
      i.set(f.id, i.get(f.id) - 1), i.get(f.id) === 0 && o.push(f);
  }
  if (r.length !== e.length)
    return console.warn("Graph has a cycle! Min-max distance root undefined."), e[0];
  const a = /* @__PURE__ */ new Map();
  for (let s = r.length - 1; s >= 0; s--) {
    const f = r[s];
    let w = 0;
    for (const m of n.get(f.id))
      w = Math.max(w, 1 + (a.get(m.id) || 0));
    a.set(f.id, w);
  }
  let l = null, h = 1 / 0;
  for (const s of e) {
    const f = a.get(s.id);
    f < h && (h = f, l = s);
  }
  return l ?? e[0];
}
function bl(e, t) {
  const n = /* @__PURE__ */ new Map(), i = /* @__PURE__ */ new Map();
  for (const s of e)
    n.set(s.id, []), i.set(s.id, 0);
  for (const s of t)
    s.directed !== !1 && (n.get(s.from.id).push(s.to), i.set(s.to.id, (i.get(s.to.id) || 0) + 1));
  const r = [], o = e.filter((s) => i.get(s.id) === 0);
  for (; o.length; ) {
    const s = o.shift();
    r.push(s);
    for (const f of n.get(s.id))
      i.set(f.id, i.get(f.id) - 1), i.get(f.id) === 0 && o.push(f);
  }
  if (r.length !== e.length)
    return console.warn("Graph has a cycle! Cannot minimize DAG height."), e[0];
  const a = /* @__PURE__ */ new Map();
  for (let s = r.length - 1; s >= 0; s--) {
    const f = r[s];
    let w = 0;
    for (const m of n.get(f.id))
      w = Math.max(w, 1 + (a.get(m.id) ?? 0));
    a.set(f.id, w);
  }
  let l = null, h = 1 / 0;
  for (const s of e) {
    const f = a.get(s.id);
    f < h && (h = f, l = s);
  }
  return l ?? e[0];
}
const Xn = {
  type: "tree",
  rootId: void 0,
  rootIdAlgorithmFinder: "MaxReachability",
  strength: 0.25,
  radial: !1,
  radialGap: 750,
  horizontal: !1,
  flipEdgeDirection: !1
};
class it {
  constructor(t, n, i, r = {}) {
    E(this, "graph");
    E(this, "simulation");
    E(this, "simulationForces");
    E(this, "options");
    E(this, "originalForceStrength");
    E(this, "canvasBCR");
    E(this, "levels");
    E(this, "positionedNodesByID");
    this.graph = t, this.simulation = n, this.simulationForces = i, this.options = dn({}, Xn, r), this.originalForceStrength = {
      link: this.simulationForces.link.strength(),
      charge: this.simulationForces.charge.strength(),
      gravity: this.simulationForces.gravity.strength()
    }, this.positionedNodesByID = /* @__PURE__ */ new Map(), this.levels = /* @__PURE__ */ new Map();
    const o = this.graph.getNodes(), a = this.options.flipEdgeDirection ? this.flipEdgeDirection(this.graph.getEdges()) : this.graph.getEdges();
    if (gn(o, a)) {
      this.graph.notifier.warning("Tree layout unavailable", "The graph contains a cycle, so it cannot be displayed as a tree.");
      return;
    }
    this.setSizes(), this.update(), this.registerForces();
  }
  update() {
    const t = this.graph.getNodes(), n = this.options.flipEdgeDirection ? this.flipEdgeDirection(this.graph.getEdges()) : this.graph.getEdges(), { levels: i } = this.buildLevels(t, n, void 0, this.options.rootIdAlgorithmFinder), { nodes: r, nodeById: o } = this.buildTree(t, n, this.options, this.canvasBCR);
    this.positionedNodesByID = o, this.levels = i, r && this.setNodePositions(r, this.options);
  }
  flipEdgeDirection(t) {
    return t.forEach((n) => {
      const i = n.from;
      n.setFrom(n.to), n.setTo(i);
    }), t;
  }
  setSizes() {
    const t = this.graph.renderer.getCanvas();
    if (!t)
      throw new Error("Canvas element is not defined in the graph renderer.");
    this.canvasBCR = t.getBoundingClientRect();
  }
  setNodePositions(t, n) {
    for (const i of t) {
      const r = this.graph.getMutableNode(i.data.id);
      if (r)
        if (n.radial) {
          const o = i.x ?? 0, a = i.y ?? 0;
          r.x = a * Math.cos(o - Math.PI / 2), r.y = a * Math.sin(o - Math.PI / 2), r.fx = r.x, r.fy = r.y;
        } else n.horizontal ? (r.x = i.y, r.fx = i.y, r.y = i.x, delete r.fy) : (r.x = i.x, r.y = i.y, r.fy = i.y, delete r.fx);
    }
  }
  unsetNodePositions() {
    this.graph.getMutableNodes().forEach((t) => {
      delete t.fy, delete t.fx;
    });
  }
  registerForces() {
    const t = this.options.strength ?? 0.1;
    if (this.options.radial) {
      const n = bi(
        (i) => (this.levels.get(i.id) ?? 1) * 100,
        0,
        0
      ).strength(t);
      this.simulation.force("tree-radial", n);
    } else
      this.simulation.force("tree-y", Ti((n) => {
        var i, r;
        return this.options.horizontal ? ((i = this.positionedNodesByID.get(n.id)) == null ? void 0 : i.x) ?? 0 : ((r = this.positionedNodesByID.get(n.id)) == null ? void 0 : r.y) ?? 0;
      }).strength(t)), this.simulation.force("tree-x", wi((n) => {
        var i, r;
        return this.options.horizontal ? ((i = this.positionedNodesByID.get(n.id)) == null ? void 0 : i.y) ?? 0 : ((r = this.positionedNodesByID.get(n.id)) == null ? void 0 : r.x) ?? 0;
      }).strength(t));
    it.adjustOtherSimulationForces(this.simulationForces, this.options);
  }
  unregisterLayout() {
    this.unregisterForces(), this.unsetNodePositions();
  }
  unregisterForces() {
    this.simulation.force("tree-radial", null), this.simulation.force("tree-y", null), this.simulation.force("tree-x", null), it.resetOtherSimulationForces(this.simulationForces, this.originalForceStrength);
  }
  static registerForcesOnSimulation(t, n, i, r, o, a, l = this) {
    const h = dn({}, Xn, o), s = h.strength ?? 0.1, f = a.width, w = a.height, m = [f / 2, w / 2];
    if (gn(t, n))
      return;
    const { levels: p } = l.buildLevelsStatic(t, n, void 0, h.rootIdAlgorithmFinder), { nodeById: A } = l.buildTreeStatic(t, n, h, a);
    if (h.radial) {
      const y = bi(
        (_) => (p.get(_.id) ?? 1) * 100,
        m[0],
        m[1]
      ).strength(s);
      i.force("tree-radial", y);
    } else
      i.force("tree-y", Ti((y) => {
        var _, v;
        return h.horizontal ? ((_ = A.get(y.id)) == null ? void 0 : _.x) ?? 0 : ((v = A.get(y.id)) == null ? void 0 : v.y) ?? 0;
      }).strength(s)), i.force("tree-x", wi((y) => {
        var _, v;
        return h.horizontal ? ((_ = A.get(y.id)) == null ? void 0 : _.y) ?? 0 : ((v = A.get(y.id)) == null ? void 0 : v.x) ?? 0;
      }).strength(s));
    l.adjustOtherSimulationForces(r, h);
  }
  static adjustOtherSimulationForces(t, n) {
    n != null && n.radial ? (t.link.strength(0), t.charge.strength(0), t.gravity.strength(0)) : (t.link.strength(0), t.charge.strength(0), t.gravity.strength(1e-5));
  }
  static resetOtherSimulationForces(t, n) {
    t.link.strength(n.link), t.charge.strength(n.charge), t.gravity.strength(n.gravity);
  }
  static simulationDone(t, n, i, r) {
    const o = dn({}, Xn, r);
    for (const a of t)
      o.radial ? (a.fx = a.x, a.fy = a.y) : o.horizontal ? (a.fx = a.x, delete a.fy) : (a.fy = a.y, delete a.fx);
  }
  buildTree(t, n, i, r) {
    return it.buildTreeStatic(t, n, i, r);
  }
  static buildTreeStatic(t, n, i, r) {
    if (!t.length)
      return {
        root: null,
        nodes: [],
        nodeById: /* @__PURE__ */ new Map()
      };
    if (gn(t, n))
      return console.warn("Cycle detected in graph. Tree layout will not be computed."), {
        root: null,
        nodes: [],
        nodeById: /* @__PURE__ */ new Map()
      };
    const o = /* @__PURE__ */ new Map();
    for (const y of t) {
      const _ = y;
      _.children = [], o.set(y.id, _);
    }
    for (const y of n) {
      const _ = o.get(y.source.id), v = o.get(y.target.id);
      _ && v && (_.children.push(v), v.parent = _);
    }
    const a = i.rootId || it.findRootId(t, n, i.rootIdAlgorithmFinder), l = o.get(a);
    if (!l)
      throw new Error(`Root node with id "${a}" not found.`);
    const h = i.radialGap, s = i.radial ? 2 * Math.PI : r.width, f = i.radial ? h : r.height, w = hr();
    i.radial ? w.size([s, f]) : w.size([s, f]).separation((y, _) => {
      var S, x;
      const v = ((x = (S = y.parent) == null ? void 0 : S.children) == null ? void 0 : x.length) ?? 1;
      return y.parent === _.parent ? 1.5 / v : 1.5;
    });
    const m = En(l), p = w(m), A = /* @__PURE__ */ new Map();
    return p.descendants().forEach((y) => {
      A.set(y.data.id, y);
    }), {
      root: p,
      nodes: p.descendants(),
      nodeById: A
    };
  }
  buildLevels(t, n, i, r) {
    return it.buildLevelsStatic(t, n, i, r);
  }
  /**
   * Builds a mapping from node ID to its level (distance from the root),
   * by traversing the graph in BFS manner. If the graph contains cycles,
   * each node is assigned the shortest level found first.
   *
   * @param nodes - The list of graph nodes.
   * @param edges - The list of graph edges (assumed to be directed).
   * @param passedRootId - The ID of the node considered as the root.
   * @param rootIdAlgorithmFinder - The algorithm to use to find the root ID.
   * @returns A mapping of each node's ID to its depth level in the tree and the maximum depth
   */
  static buildLevelsStatic(t, n, i, r) {
    var m;
    if (!t.length)
      return {
        levels: /* @__PURE__ */ new Map(),
        maxDepth: 0,
        nodeCountPerLevel: {}
      };
    const o = i || it.findRootId(t, n, r), a = /* @__PURE__ */ new Map([[o, 0]]), l = /* @__PURE__ */ new Map();
    for (const p of t)
      l.set(p.id, []);
    for (const { source: p, target: A } of n)
      (m = l.get(p.id)) == null || m.push(A.id);
    const h = [o];
    let s = 0;
    for (; s < h.length; ) {
      const p = h[s++], A = a.get(p) ?? 0;
      for (const y of l.get(p) ?? [])
        a.has(y) || (a.set(y, A + 1), h.push(y));
    }
    let f = 0;
    const w = {};
    for (const p of a.values())
      p > f && (f = p), w[p] = (w[p] || 0) + 1;
    return {
      levels: a,
      maxDepth: f,
      nodeCountPerLevel: w
    };
  }
  /**
   * Attempts to infer the root node of a directed graph.
   *
   * This function looks for a node that is never a target in the list of links,
   * assuming such a node is a likely root (i.e., has no incoming edges).
   * If no such node is found, it falls back to the first node in the list.
   *
   * @param nodes - The list of graph nodes.
   * @param edges - The list of graph edges (assumed to be directed).
   * @returns The ID of the inferred root node.
   */
  static findRootId(t, n, i) {
    switch (i) {
      case "FirstZeroInDegree":
        return Ui(t, n).id;
      case "MaxReachability":
        return _l(t, n).id;
      case "MinMaxDistance":
        return vl(t, n).id;
      case "MinHeight":
        return bl(t, n).id;
      default:
        return Ui(t, n).id;
    }
  }
}
class Ve extends it {
  constructor(t, n, i, r) {
    super(t, n, i, {
      ...r,
      type: "tree"
    });
  }
  static registerForcesOnSimulation(t, n, i, r, o, a) {
    it.registerForcesOnSimulation(
      t,
      n,
      i,
      r,
      o,
      a,
      Ve
    );
  }
  buildTree(t, n, i, r) {
    return Ve.buildTreeStatic(t, n, i, r);
  }
  static buildTreeStatic(t, n, i, r) {
    if (!t.length)
      return {
        root: null,
        nodes: [],
        nodeById: /* @__PURE__ */ new Map()
      };
    if (gn(t, n))
      return console.warn("Cycle detected in graph. Tree layout will not be computed."), {
        root: null,
        nodes: [],
        nodeById: /* @__PURE__ */ new Map()
      };
    const o = /* @__PURE__ */ new Map();
    for (const y of t) {
      const _ = y;
      _.children = [], o.set(y.id, _);
    }
    if (!i.rootId || !o.get(i.rootId))
      throw new Error("Ego Tree can only be created with a rootId");
    const a = i.rootId, l = o.get(a);
    if (l.children = [], !l)
      throw new Error(`Root node with id "${a}" not found.`);
    for (const y of n) {
      const _ = o.get(y.source.id), v = o.get(y.target.id);
      _ && v && (y.source.id === l.id ? (l.children.push(v), v.parent = l) : y.target.id === l.id && (l.children.push(_), _.parent = l));
    }
    const h = i.radialGap, s = i.radial ? 2 * Math.PI : r.width, f = i.radial ? h : r.height, w = hr();
    i.radial ? w.size([s, f]) : w.size([s, f]).separation((y, _) => {
      var S, x;
      const v = ((x = (S = y.parent) == null ? void 0 : S.children) == null ? void 0 : x.length) ?? 1;
      return y.parent === _.parent ? 1.5 / v : 1.5;
    });
    const m = En(l), p = w(m), A = /* @__PURE__ */ new Map();
    return p.descendants().forEach((y) => {
      A.set(y.data.id, y);
    }), {
      root: p,
      nodes: p.descendants(),
      nodeById: A
    };
  }
}
function wl(e) {
  var n;
  const t = (n = e.getData()) == null ? void 0 : n.label;
  return typeof t == "string" ? t : "";
}
const re = {
  d3Alpha: 1,
  d3AlphaMin: 1e-3,
  d3AlphaDecay: 0.05,
  d3AlphaTarget: 0,
  d3VelocityDecay: 0.45,
  d3LinkDistance: 40,
  d3LinkStrength: null,
  d3ManyBodyStrength: -150,
  d3ManyBodyTheta: 0.9,
  d3CollideRadius: 12,
  d3CollideRadiusMultiplier: 1.2,
  d3CollideStrength: 1,
  d3CollideIterations: 1,
  d3GravityStrength: 0.1,
  d3GravityStrengthConnected: 1e-3,
  enabled: !0,
  cooldownTime: 2e3,
  useWorker: !0,
  warmupTicks: "auto",
  freezeNodesOnDrag: !0,
  gridSnappingEnabled: !1,
  gridSize: 50,
  fitViewOnExpandCollapse: !1,
  layout: {
    type: "force"
  },
  callbacks: {
    onInit: () => {
    },
    onStart: () => {
    },
    onStop: () => {
    },
    onTick: () => {
    }
  }
}, dt = {
  repulsion: [0, 100],
  linkDistance: [40, 260],
  collisionRadius: [4, 60],
  friction: [0, 100]
}, Tl = {
  tight: { repulsion: 32, linkDistance: 70, collisionRadius: 16, friction: 58 },
  loose: { repulsion: 70, linkDistance: 150, collisionRadius: 26, friction: 28 },
  default: { repulsion: 70, linkDistance: 150, collisionRadius: 26, friction: 28 }
}, O = class O {
  // friction 0..100 → velocityDecay
  constructor(t, n = {}) {
    E(this, "simulation");
    E(this, "graph");
    E(this, "canvas");
    E(this, "graphInteraction");
    E(this, "layout");
    E(this, "canvasBCR");
    E(this, "animationFrameId", null);
    E(this, "startSimulationTime", 0);
    E(this, "engineRunning", !1);
    E(this, "slowTickThresholdReached", !1);
    E(this, "avgTickDuration", 0);
    E(this, "SLOW_TICK_THRESHOLD", 33);
    // ms of tick compute+render (≈30fps budget)
    E(this, "dragInProgress", !1);
    E(this, "dragSelection", []);
    E(this, "totalTickCount", 0);
    E(this, "options");
    E(this, "callbacks");
    E(this, "simulationForces");
    E(this, "scaledForces", {
      d3ManyBodyStrength: re.d3ManyBodyStrength,
      d3CollideStrength: re.d3CollideStrength
    });
    /** Current abstract physics-knob values (what the View flyout renders). */
    E(this, "physicsKnobs");
    if (this.graph = t, this.options = dn({}, re, n), this.callbacks = this.options.callbacks ?? {}, this.physicsKnobs = O.knobsFromOptions(this.options), this.canvas = this.graph.renderer.getCanvas(), !this.canvas) throw new Error("Canvas element is not defined in the graph renderer.");
    if (this.canvasBCR = this.canvas.getBoundingClientRect(), this.graphInteraction = this.graph.renderer.getGraphInteraction(), !this.graphInteraction) throw new Error("Graph interaction is not available.");
    const i = O.initSimulationForces(this.options, this.canvasBCR);
    this.simulation = i.simulation, this.simulationForces = i.simulationForces, this.scaledForces.d3ManyBodyStrength = this.options.d3ManyBodyStrength || re.d3ManyBodyStrength, this.scaledForces.d3CollideStrength = this.options.d3CollideStrength || re.d3CollideStrength, this.options.layout.type === "tree" ? this.layout = new it(
      this.graph,
      this.simulation,
      this.simulationForces,
      this.options.layout
    ) : this.options.layout.type === "egoTree" && (this.layout = new Ve(
      this.graph,
      this.simulation,
      this.simulationForces,
      this.options.layout
    )), this.callbacks.onInit && this.callbacks.onInit(this);
  }
  /** @private */
  static initSimulationForces(t, n) {
    const i = {
      link: to(),
      charge: yo(),
      collide: Qr(),
      gravity: _o()
      // clusterRadialConstraint: ForceClusterRadial(),
    }, r = mo().force("link", i.link).force("charge", i.charge).force("collide", i.collide).force("gravity", i.gravity);
    return this.initSimulationForceGravity(i.gravity, t, n), this.initSimulationForceLink(i.link, t), this.initSimulationForceCharge(i.charge, t), this.initSimulationForceCollide(i.collide, t), r.alphaMin(t.d3AlphaMin), r.alphaDecay(t.d3AlphaDecay), r.alphaTarget(0), r.velocityDecay(t.d3VelocityDecay), {
      simulation: r,
      simulationForces: i
    };
  }
  static initSimulationForceGravity(t, n, i) {
    t.x(i.width / 2).y(i.height / 2).strength((r) => (r.degree() ?? 0) === 0 ? n.d3GravityStrength : n.d3GravityStrengthConnected);
  }
  static initSimulationForceLink(t, n) {
    t.distance((i) => {
      const r = i.__clusterAnchorDistance;
      if (r != null) return r;
      const o = wl(i);
      if (!o || o === "")
        return n.d3LinkDistance;
      const a = o.length * 10;
      return Math.max(n.d3LinkDistance, a);
    }), n.d3LinkStrength && t.strength(n.d3LinkStrength);
  }
  static initSimulationForceCharge(t, n) {
    t.theta(n.d3ManyBodyTheta).strength((i) => {
      const r = i, o = n.d3ManyBodyStrength, a = r.expanded ? r.getCircleRadiusCollapsed() : r.getCircleRadius(), l = 10 + Math.sqrt(Math.max(0, a - 10));
      let h = r.weight ?? 1;
      return h *= r.isParent ? 10 : 1, o * (l * l) / 100 * h;
    });
  }
  static initSimulationForceCollide(t, n) {
    const i = n.d3CollideRadiusMultiplier;
    t.radius((r) => {
      const o = r;
      return o.expanded ? i * o.getCircleRadius() + 20 : o.getCircleRadius() ? i * o.getCircleRadius() : n.d3CollideRadius;
    }).strength(n.d3CollideStrength);
  }
  static initSimulationForceClusterRadialConstraint(t, n) {
    t.strength(n.d3CollideStrength);
  }
  update() {
    this.layout && this.layout.update();
    const t = this.graph.getMutableNodes().filter((i) => i.visible);
    this.simulation.nodes(t);
    const n = this.simulation.force("link");
    n && n.id((i) => i.id).links(this.getActiveEdges()), this.restart();
  }
  /** @private */
  getActiveEdges() {
    const t = new Set(
      this.graph.getMutableNodes().filter((a) => a.visible).map((a) => a.id)
    ), n = (a) => {
      let l = a;
      for (; l && !t.has(l.id); ) l = l.parentNode;
      return l;
    }, i = (a, l) => a < l ? `${a}|${l}` : `${l}|${a}`, r = [], o = /* @__PURE__ */ new Set();
    for (const a of this.graph.getMutableEdges()) {
      if (!a.visible) continue;
      const l = a.source, h = a.target;
      if (!l.isChild && !h.isChild) {
        r.push(a), o.add(i(l.id, h.id));
        continue;
      }
      if (l.isChild && h.isChild) continue;
      const s = l.isChild ? h : l, f = n(l.isChild ? l : h);
      if (!f || f.id === s.id) continue;
      const w = i(s.id, f.id);
      o.has(w) || (o.add(w), r.push(this.clusterAnchorLink(s, f)));
    }
    return r;
  }
  /**
   * A force-only link tying an external node to an expanded cluster it connects
   * into. Not a real Edge — never rendered, never registered on the nodes — just
   * the `{source, target, distance}` the link force needs. Its distance is the
   * cluster radius (plus the base link distance) so the node rests outside the bubble.
   * @private
   */
  clusterAnchorLink(t, n) {
    return {
      id: `cluster-anchor-${t.id}-${n.id}`,
      source: t,
      target: n,
      __clusterAnchorDistance: n.getCircleRadius() + this.options.d3LinkDistance
    };
  }
  /** @private */
  scaleSimulationOptions() {
    const t = O.scaleSimulationOptions(this.options, this.canvasBCR, this.graph.getNodeCount());
    this.scaledForces.d3ManyBodyStrength = t.d3ManyBodyStrength ?? re.d3ManyBodyStrength, this.scaledForces.d3CollideStrength = t.d3CollideStrength ?? re.d3CollideStrength;
  }
  /** @private */
  static scaleSimulationOptions(t, n, i) {
    const r = i / (n.width * n.height), o = Math.min(2, 75e-6 / r);
    return {
      d3ManyBodyStrength: t.d3ManyBodyStrength * o,
      d3CollideStrength: t.d3ManyBodyStrength * o
    };
  }
  /** @private */
  applyScalledSimulationOptions() {
    O.initSimulationForceCharge(this.simulationForces.charge, this.options), O.initSimulationForceCollide(this.simulationForces.collide, this.options);
  }
  enable() {
    this.avgTickDuration = 0, this.options.enabled = !0, this.start(!1);
  }
  disable() {
    this.options.enabled = !1, this.stop();
  }
  /**
   * Pause the simulation
   */
  pause() {
    this.engineRunning = !1, this.slowTickThresholdReached = !1;
  }
  /**
   * Restart the simulation with rendering on each animation frame.
   */
  restart() {
    this.startSimulationTime = (/* @__PURE__ */ new Date()).getTime(), this.engineRunning = !0, this.slowTickThresholdReached = !1;
  }
  /**
   * Start the simulation with rendering on each animation frame.
   */
  async start(t = !0) {
    if (t && await this.runSimulationWorkerRouter(), !this.options.enabled) {
      this.engineRunning = !1;
      return;
    }
    this.engineRunning = !0, this.slowTickThresholdReached = !1, this.callbacks.onStart && this.callbacks.onStart(this), this.animationFrameId === null && this.startAnimationLoop();
  }
  /**
   * Manually stop the simulation and cancel animation frame.
   */
  stop() {
    this.engineRunning = !1, this.animationFrameId !== null && (cancelAnimationFrame(this.animationFrameId), this.animationFrameId = null), this.simulation.stop(), this.callbacks.onStop && this.callbacks.onStop(this);
  }
  /**
   * Start the simulation loop with rendering on each animation frame.
   */
  startAnimationLoop() {
    const t = () => {
      this.animationFrameId = requestAnimationFrame(t), this.simulationTick();
    };
    this.engineRunning = !0, this.simulation.alpha(0.01).restart(), this.animationFrameId = requestAnimationFrame(t);
  }
  /**
   * Evaluate at each tick to update the simulation state and request rendering
   */
  simulationTick() {
    if (this.engineRunning) {
      !this.dragInProgress && ((/* @__PURE__ */ new Date()).getTime() - this.startSimulationTime > this.options.cooldownTime || this.options.d3AlphaMin > 0 && this.simulation.alpha() < this.options.d3AlphaMin) && (this.engineRunning = !1, this.simulation.stop(), this.callbacks.onStop && this.callbacks.onStop(this)), this.totalTickCount++;
      const t = performance.now();
      this.simulation.tick(), this.graph.nextTick(), this.updateTickMetrics(performance.now() - t), this.callbacks.onTick && this.callbacks.onTick(this), this.graphInteraction.simulationTick(), this.totalTickCount % 10 === 0 && this.graphInteraction.simulationSlowTick();
    }
  }
  updateTickMetrics(t) {
    var n;
    this.avgTickDuration = this.avgTickDuration * 0.9 + t * 0.1, this.avgTickDuration > this.SLOW_TICK_THRESHOLD && (this.slowTickThresholdReached = !0, this.disable(), this.graph.UIManager.showNotification({
      level: "warning",
      title: "Physics engine running slow",
      message: "The physic has been disabled."
    }), (n = this.graph.UIManager.viewFlyout) == null || n.syncRunState());
  }
  /**
   * Returns a promise that resolves when the simulation stops naturally.
   * Useful for performing actions (like fitAndCenter) after stabilization.
   */
  async waitForSimulationStop() {
    if (this.engineRunning)
      return new Promise((t) => {
        const n = this.callbacks.onStop;
        this.callbacks.onStop = (i) => {
          n == null || n(i), this.callbacks.onStop = n, t();
        };
      });
  }
  isEnabled() {
    return this.options.enabled;
  }
  // Match computed positions to live nodes by id: the layout is handed a
  // different (and differently ordered) node set than the full node map, so
  // they can't be aligned by array index.
  applyComputedPositions(t) {
    const n = new Map(t.map((i) => [i.id, i]));
    for (const i of this.graph.getMutableNodes()) {
      const r = n.get(i.id);
      r && (i.x = r.x, i.y = r.y, i.fx = typeof r.fx == "number" ? r.fx : void 0, i.fy = typeof r.fy == "number" ? r.fy : void 0);
    }
  }
  async computeGraph(t = {}) {
    var f;
    const { runSimulation: n } = await Promise.resolve().then(() => Al), i = (f = this.canvas) == null ? void 0 : f.getBoundingClientRect();
    if (!i) return;
    const r = this.graph.getMutableNodes(), o = this.graph.getNodes(), a = this.graph.getEdges(), { callbacks: l, ...h } = this.options;
    Object.assign(h, t);
    const { nodes: s } = n(
      o,
      a,
      h,
      i
    );
    this.applyComputedPositions(s), this.graph.updateData(r, void 0, !1);
  }
  async runSimulationWorkerRouter(t = {}) {
    if (this.options.useWorker)
      try {
        await this.runSimulationWorker(t);
        return;
      } catch (n) {
        this.options.useWorker = !1, console.warn(
          "[Pivotick] Simulation Web Worker unavailable (often a CSP blocking blob workers); falling back to the main thread. Set `simulation.useWorker: false` to silence this.",
          n
        );
      }
    await this.computeGraph(t), this.graph.updateLayoutProgress(100, 0, "done");
  }
  async runSimulationWorker(t = {}) {
    var f;
    const n = (f = this.canvas) == null ? void 0 : f.getBoundingClientRect();
    if (!n) return;
    const i = this.graph.getMutableNodes(), r = this.graph.getNodes().map((w) => w.toSimulationDTO()), o = this.graph.getEdges().map((w) => w.toSimulationDTO()), a = (w, m) => {
      this.graph.updateLayoutProgress(w, m, "simulation");
    }, { callbacks: l, ...h } = this.options;
    Object.assign(h, t);
    const { nodes: s } = await Ga(
      r,
      o,
      h,
      n,
      a
    );
    this.graph.updateLayoutProgress(100, 0, "rendering"), this.applyComputedPositions(s), this.graph.updateData(i, void 0, !1), this.graph.updateLayoutProgress(100, 0, "done");
  }
  /**
   * Restart the simulation with a bit of heat
   */
  reheat(t = 0.7) {
    this.restart(), this.simulation.alpha(t).restart();
  }
  /**
   * Re-read the node-dependent force accessors and reheat.
   *
   * d3-force caches per-node radius/strength when a force is initialised (i.e.
   * when nodes are set), not on every tick — so mutating a node's radius after
   * the sim is running has no effect until the forces are re-initialised.
   * Re-setting the nodes does that; the reheat then lets collision/charge
   * re-lay-out with the new sizes. Used when a custom node measures its size
   * after the initial layout has already cooled. No-op when disabled.
   */
  refreshForcesAndReheat(t = 0.5) {
    if (!this.options.enabled) return;
    const n = this.graph.getMutableNodes().filter((i) => i.visible);
    this.simulation.nodes(n), this.reheat(t);
  }
  // ─── Physics knobs (View flyout) ────────────────────────────────────────────
  // Each setter takes an abstract knob value (range in PHYSICS_KNOB_RANGES), maps
  // it onto a d3-force domain, re-initialises the affected force so d3 re-reads its
  // cached per-node array, then reheats. Reheat is skipped while physics is disabled;
  // the value is still stored so it takes effect once physics is re-enabled.
  /** Push-apart strength. Knob 0–100 → d3ManyBodyStrength. */
  setRepulsion(t) {
    const n = O.clamp(t, dt.repulsion);
    this.physicsKnobs.repulsion = n, this.options.d3ManyBodyStrength = O.mapLinear(n, dt.repulsion, O.REPULSION_STRENGTH_RANGE), this.scaledForces.d3ManyBodyStrength = this.options.d3ManyBodyStrength, O.initSimulationForceCharge(this.simulationForces.charge, this.options), this.reheatIfEnabled();
  }
  /** Preferred edge length. Knob 40–260 (px) → d3LinkDistance. */
  setLinkDistance(t) {
    const n = O.clamp(t, dt.linkDistance);
    this.physicsKnobs.linkDistance = n, this.options.d3LinkDistance = O.mapLinear(n, dt.linkDistance, O.LINK_DISTANCE_RANGE), O.initSimulationForceLink(this.simulationForces.link, this.options), this.reheatIfEnabled();
  }
  /** Node spacing. Knob 4–60 → d3CollideRadiusMultiplier (scales each node's collision radius). */
  setCollisionRadius(t) {
    const n = O.clamp(t, dt.collisionRadius);
    this.physicsKnobs.collisionRadius = n, this.options.d3CollideRadiusMultiplier = O.mapLinear(n, dt.collisionRadius, O.COLLIDE_MULTIPLIER_RANGE), O.initSimulationForceCollide(this.simulationForces.collide, this.options), this.reheatIfEnabled();
  }
  /** Motion damping. Knob 0–100 → d3VelocityDecay (÷100). Applied live each tick — no reheat. */
  setFriction(t) {
    const n = O.clamp(t, dt.friction);
    this.physicsKnobs.friction = n, this.options.d3VelocityDecay = O.mapLinear(n, dt.friction, O.FRICTION_DECAY_RANGE), this.simulation.velocityDecay(this.options.d3VelocityDecay);
  }
  /** Apply a named preset ({@link PHYSICS_PRESETS}): sets all four knobs and reheats once. */
  applyPhysicsPreset(t) {
    const n = Tl[t];
    this.physicsKnobs = { ...n }, this.options.d3ManyBodyStrength = O.mapLinear(n.repulsion, dt.repulsion, O.REPULSION_STRENGTH_RANGE), this.scaledForces.d3ManyBodyStrength = this.options.d3ManyBodyStrength, this.options.d3LinkDistance = O.mapLinear(n.linkDistance, dt.linkDistance, O.LINK_DISTANCE_RANGE), this.options.d3CollideRadiusMultiplier = O.mapLinear(n.collisionRadius, dt.collisionRadius, O.COLLIDE_MULTIPLIER_RANGE), this.options.d3VelocityDecay = O.mapLinear(n.friction, dt.friction, O.FRICTION_DECAY_RANGE), O.initSimulationForceCharge(this.simulationForces.charge, this.options), O.initSimulationForceLink(this.simulationForces.link, this.options), O.initSimulationForceCollide(this.simulationForces.collide, this.options), this.simulation.velocityDecay(this.options.d3VelocityDecay), this.reheatIfEnabled();
  }
  /** Current knob values, for seeding the View-flyout sliders. */
  getPhysicsKnobs() {
    return { ...this.physicsKnobs };
  }
  /** The active layout type — the View flyout greys out physics under non-`force` layouts. */
  getLayoutType() {
    return this.options.layout.type;
  }
  reheatIfEnabled(t = 0.5) {
    this.options.enabled && this.reheat(t);
  }
  static clamp(t, [n, i]) {
    return Math.max(n, Math.min(i, t));
  }
  static mapLinear(t, n, i) {
    const r = (t - n[0]) / (n[1] - n[0]);
    return i[0] + r * (i[1] - i[0]);
  }
  /** Recover the abstract knob values from a set of d3-force options (inverse of the setters). */
  static knobsFromOptions(t) {
    const n = (i, r, o) => Math.round(O.clamp(O.mapLinear(i, r, dt[o]), dt[o]));
    return {
      repulsion: n(t.d3ManyBodyStrength, O.REPULSION_STRENGTH_RANGE, "repulsion"),
      linkDistance: n(t.d3LinkDistance, O.LINK_DISTANCE_RANGE, "linkDistance"),
      collisionRadius: n(t.d3CollideRadiusMultiplier, O.COLLIDE_MULTIPLIER_RANGE, "collisionRadius"),
      friction: n(t.d3VelocityDecay, O.FRICTION_DECAY_RANGE, "friction")
    };
  }
  /**
   * @private
   */
  createDragBehavior() {
    return ca().filter(() => !this.graph.editing.connectManager.isActiveAndNotIdle()).on("start.draggedelement", (t, n) => {
      this.graphInteraction.hasActiveMultiselection() ? this.dragSelection = this.graphInteraction.getSelectedNodes().map((i) => {
        const { node: r } = i;
        return r.freeze(), {
          node: r,
          dx: r.x - n.x,
          dy: r.y - n.y
        };
      }) : (this.dragSelection = [], n.freeze());
    }).on("drag.draggedelement", (t, n) => {
      if (!this.dragInProgress && this.isEnabled() && (this.dragInProgress = !0, this.restart(), this.simulation.alphaTarget(0.3).restart()), this.graphInteraction.hasActiveMultiselection())
        this.dragSelection.forEach(({ node: i, dx: r, dy: o }) => {
          const a = this.applySnap(t.x + r), l = this.applySnap(t.y + o);
          i.fx = a, i.fy = l, i.x = a, i.y = l;
        });
      else {
        const i = this.applySnap(t.x), r = this.applySnap(t.y);
        n.fx = i, n.fy = r, n.x = i, n.y = r;
      }
      if (this.graphInteraction.dragging(t.sourceEvent, t.subject), !this.engineRunning || !this.isEnabled()) {
        const i = this.graphInteraction.hasActiveMultiselection() ? this.dragSelection.map((r) => r.node) : [n];
        this.graph.nextTickFor(i);
      }
    }).on("end.draggedelement", (t, n) => {
      !t.active && this.dragInProgress && (this.dragInProgress = !1, this.restart(), this.simulation.alphaTarget(this.options.d3AlphaTarget).restart()), this.options.freezeNodesOnDrag || (this.graphInteraction.hasActiveMultiselection() ? (this.dragSelection.forEach(({ node: i }) => i.unfreeze()), this.dragSelection = []) : n.unfreeze()), this.graphInteraction.dragended(t.sourceEvent, t.subject);
    });
  }
  isDragging() {
    return this.dragInProgress;
  }
  toggleGridSnapping() {
    this.options.gridSnappingEnabled = !this.options.gridSnappingEnabled;
  }
  toggleFreezeNodesOnDrag() {
    this.options.freezeNodesOnDrag = !this.options.freezeNodesOnDrag;
  }
  isFreezeNodesOnDrag() {
    return this.options.freezeNodesOnDrag;
  }
  isGridSnappingEnabled() {
    return this.options.gridSnappingEnabled;
  }
  toggleFitViewOnExpandCollapse() {
    this.options.fitViewOnExpandCollapse = !this.options.fitViewOnExpandCollapse;
  }
  isFitViewOnExpandCollapse() {
    return this.options.fitViewOnExpandCollapse;
  }
  applySnap(t) {
    return this.options.gridSnappingEnabled ? Math.round(t / this.options.gridSize) * this.options.gridSize : t;
  }
  /**
   * Snap a graph-space coordinate to the grid when grid-snapping is enabled
   * (a no-op otherwise). Public so non-simulation draggables (e.g. notes) can
   * snap on the same grid as nodes.
   */
  snapToGrid(t) {
    return this.applySnap(t);
  }
  getForceSimulation() {
    return this.simulationForces;
  }
  getSimulation() {
    return this.simulation;
  }
  /**
   * Allows to change the layout of the graph
   * 
   * @example
   * ```ts
   * changeLayout('tree', {
   *     layout: {
   *          horizontal: false,
   *          rootIdAlgorithmFinder: 'FirstZeroInDegree'
   *     }
   * })
   * ```
   */
  async changeLayout(t, n = {}) {
    var i;
    this.layout && ((i = this.layout) == null || i.unregisterLayout(), this.layout = void 0), n = n ?? {}, n.layout = n.layout ?? {}, n.layout.type = t, t === "force" ? this.applyScalledSimulationOptions() : t === "tree" && (this.layout = new it(this.graph, this.simulation, this.simulationForces, n.layout)), this.options.layout.type = t, this.update(), this.pause(), await this.runSimulationWorkerRouter(n), this.restart(), await this.waitForSimulationStop(), this.graph.renderer.fitAndCenterWhenSettled();
  }
};
// d3-force domains each knob maps onto; the knob's own range is in PHYSICS_KNOB_RANGES.
E(O, "REPULSION_STRENGTH_RANGE", [0, -400]), // repulsion 0..100 (more negative = stronger)
E(O, "LINK_DISTANCE_RANGE", [40, 260]), // linkDistance 40..260 (identity, px)
E(O, "COLLIDE_MULTIPLIER_RANGE", [0.6, 2.4]), // collisionRadius 4..60
E(O, "FRICTION_DECAY_RANGE", [0, 1]);
let bn = O;
const fr = 1e4, wn = 2e4, Tn = 0.15 * wn;
self.onmessage = (e) => {
  var y, _, v, S;
  if (e.data.source !== "simulation-worker-wrapper") return;
  const { nodes: t, edges: n, options: i, canvasBCR: r } = e.data, o = t.map((x) => {
    const g = new lr(x.id, x.data, x.style);
    return g.setCircleRadius(x._circleRadius ?? 10), typeof x.x == "number" && (g.x = x.x), typeof x.y == "number" && (g.y = x.y), typeof x.fx == "number" && (g.fx = x.fx), typeof x.fy == "number" && (g.fy = x.fy), g;
  }), a = new Map(o.map((x) => [x.id, x]));
  (y = i.layout) == null || y.type;
  const { simulation: l, simulationForces: h } = bn.initSimulationForces(i, r), s = [];
  for (const x of n) {
    const g = a.get(x.from.id), I = a.get(x.to.id);
    if (g && I) {
      const C = x.style ?? {};
      s.push(new xn(x.id, g, I, x.data, C, x.directed));
    }
  }
  l.nodes(o);
  const f = l.force("link");
  f && f.id((x) => x.id).links(s), ((_ = i.layout) == null ? void 0 : _.type) === "tree" ? it.registerForcesOnSimulation(
    o,
    s,
    l,
    h,
    i.layout,
    r,
    it
  ) : ((v = i.layout) == null ? void 0 : v.type) === "egoTree" && it.registerForcesOnSimulation(
    o,
    s,
    l,
    h,
    i.layout,
    r,
    Ve
  );
  let w = i.warmupTicks || wn;
  w = w === "auto" ? wn : w, w = w - Tn;
  let m = 0.3;
  l.alphaTarget(m);
  const p = (/* @__PURE__ */ new Date()).getTime();
  let A;
  for (let x = 0; x < w && !((/* @__PURE__ */ new Date()).getTime() - p > fr || (/* @__PURE__ */ new Date()).getTime() - p > i.cooldownTime || Sn(i, l, m) && (/* @__PURE__ */ new Date()).getTime() - p > i.cooldownTime * 0.15); ++x)
    x % 5 === 0 && (A = ji(x, (/* @__PURE__ */ new Date()).getTime() - p, i), postMessage({ type: "tick", progress: A, elapsedTime: (/* @__PURE__ */ new Date()).getTime() - p })), l.tick();
  m = 0, l.alphaTarget(m), l.alpha(1);
  for (let x = 0; x < Tn && !(Sn(i, l, m) && (/* @__PURE__ */ new Date()).getTime() - p > i.cooldownTime * 0.15); ++x)
    l.tick(), x % 5 === 0 && (A = ji(w + x, (/* @__PURE__ */ new Date()).getTime() - p, i), postMessage({ type: "tick", progress: A, elapsedTime: (/* @__PURE__ */ new Date()).getTime() - p }));
  postMessage({ type: "tick", progress: 1, elapsedTime: (/* @__PURE__ */ new Date()).getTime() - p }), ((S = i.layout) == null ? void 0 : S.type) === "tree" && it.simulationDone(
    o,
    s,
    l,
    i.layout
  ), postMessage({
    type: "done",
    nodes: o.map((x) => x.toDict()),
    edges: s.map((x) => x.toDict())
  });
};
function Sl(e, t, n, i) {
  var p, A, y, _;
  const r = e.map((v) => {
    const S = new lr(v.id, v.getData(), v.getStyle());
    return S.weight = v.weight || 1, S.setCircleRadius(v.getCircleRadius()), typeof v.x == "number" && (S.x = v.x), typeof v.y == "number" && (S.y = v.y), typeof v.fx == "number" && (S.fx = v.fx), typeof v.fy == "number" && (S.fy = v.fy), S;
  }), o = new Map(r.map((v) => [v.id, v]));
  (p = n.layout) == null || p.type;
  const { simulation: a, simulationForces: l } = bn.initSimulationForces(n, i), h = [];
  for (const v of t) {
    const S = o.get(v.from.id), x = o.get(v.to.id);
    if (S && x) {
      const g = v.getStyle() ?? {};
      h.push(new xn(v.id, S, x, v.getData(), g, v.directed));
    }
  }
  a.nodes(r);
  const s = a.force("link");
  s && s.id((v) => v.id).links(h), (((A = n.layout) == null ? void 0 : A.type) === "tree" || ((y = n.layout) == null ? void 0 : y.type) === "egoTree") && it.registerForcesOnSimulation(
    r,
    h,
    a,
    l,
    n.layout,
    i,
    it
  );
  let f;
  n.warmupTicks === "auto" || n.warmupTicks == null ? f = wn : f = n.warmupTicks, f = f - Tn;
  let w = 0.3;
  a.alphaTarget(w);
  const m = (/* @__PURE__ */ new Date()).getTime();
  for (let v = 0; v < f && !((/* @__PURE__ */ new Date()).getTime() - m > fr || (/* @__PURE__ */ new Date()).getTime() - m > n.cooldownTime || Sn(n, a, w) && (/* @__PURE__ */ new Date()).getTime() - m > n.cooldownTime * 0.15); ++v)
    a.tick();
  w = 0, a.alphaTarget(w), a.alpha(1);
  for (let v = 0; v < Tn && !(Sn(n, a, w) && (/* @__PURE__ */ new Date()).getTime() - m > n.cooldownTime * 0.15); ++v)
    a.tick();
  return ((_ = n.layout) == null ? void 0 : _.type) === "tree" && it.simulationDone(
    r,
    h,
    a,
    n.layout
  ), {
    nodes: r,
    edges: h
  };
}
function ji(e, t, n) {
  return t / n.cooldownTime;
}
function Sn(e, t, n) {
  return e.d3AlphaMin > 0 && t.alpha() - n < e.d3AlphaMin;
}
const Al = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  runSimulation: Sl
}, Symbol.toStringTag, { value: "Module" }));
export {
  Sl as runSimulation
};
