function isFramed() {
    var sPageURL = window.location.search.substring(1);
    var sURLVariables = sPageURL.split('&');
    for (var i = 0; i < sURLVariables.length; i++) {
        var sParameterName = sURLVariables[i].split('=');
        if (decodeURIComponent(sParameterName[0]) == "framed") {
            return sParameterName[1] == "true";
        }
    }
    return false;
}