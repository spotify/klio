// Allows for `.. container:: toggle` directives to toggle
// Adapted from https://stackoverflow.com/a/25543713
$(document).ready(function() {
    $(".admonition-collapsible > *").hide();
    $(".admonition-collapsible > .admonition-title").show();
    $(".admonition-collapsible > .admonition-title").click(function() {
        $(this).parent().children().not(".admonition-title").toggle(400);
        $(this).parent().children(".admonition-title").toggleClass("open");
    })
    $(".admonition-collapsible").click(function() {
        $(this).toggleClass("open");
    })
});
