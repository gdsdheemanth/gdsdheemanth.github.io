
var MK = function(id){ return '<defs><marker id="' + id + '" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path class="mk" d="M0,0 L10,5 L0,10 z"/></marker></defs>'; };
var FIGS = {};
/* helpers for lesson diagrams */
function B(x, y, w, h, c, lines, bold){
  var s = '<rect class="' + c + '" x="' + x + '" y="' + y + '" width="' + w + '" height="' + h + '" rx="8"/>';
  var n = lines.length, lh = 16, y0 = y + h / 2 - (n - 1) * lh / 2 + 4;
  lines.forEach(function(t, i){ s += '<text x="' + (x + w / 2) + '" y="' + (y0 + i * lh) + '" text-anchor="middle"' + (i === 0 ? (bold === false ? '' : ' class="b"') : ' class="sm"') + '>' + t + '</text>'; });
  return s;
}
function A(id, d, cls){ return '<path class="' + (cls || "ln") + '" d="' + d + '" marker-end="url(#' + id + ')"/>'; }
function T(x, y, t, c, anchor){ return '<text x="' + x + '" y="' + y + '"' + (c ? ' class="' + c + '"' : '') + (anchor ? ' text-anchor="' + anchor + '"' : '') + '>' + t + '</text>'; }
function S(id, h, label, body){ return '<svg class="dg" viewBox="0 0 760 ' + h + '" role="img" aria-label="' + label + '">' + MK(id) + body + '</svg>'; }


/* figs2: Part I (the language) diagrams, original drawings */
