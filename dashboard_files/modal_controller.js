
modal_iframe = document.getElementById("modal-iframe");

function delay(time) {
    return new Promise(resolve => setTimeout(resolve, time));
}

function setModalSource(clicked_btn) {
    modal_iframe.src = "";
    //await delay(3000);
    const ifrm = clicked_btn.parentElement.getElementsByTagName("iframe")[0];
    modal_iframe.src = ifrm.src;
}

window.addEventListener('load', function() {
    modal_buttons = document.getElementsByClassName("fullscreen-button");
    console.log(modal_buttons);
    for (btn of modal_buttons) {
        btn.onclick = function() { setModalSource(this); }
    }
});
