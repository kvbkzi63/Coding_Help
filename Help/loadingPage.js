/**
 * loading畫面
 */
class LoadingPage {
    constructor(text) {
        this.text = text || "Loading";
        let div = document.createElement("DIV");
        div.classList.add("loading-page");
        div.classList.add("visibility-hide");
        $("body").append(div);
        this.loading = $(div);
        this.loading.append("<div></div>");
        this.loadingBody = this.loading.find("div");

        let iconList = [];
        for (let i = 0; i < 12; i++) {
            iconList.push(`<div class="circle"></div>`);
        }
        this.loadingBody.append(iconList.join(""));

        this.loadingBody.append(`<span class="text">${this.text}...</span>`);
    }
    doing(callback) {
        this.loading.removeClass("visibility-hide");
        if (callback) {
            window.setTimeout(callback, 500);
        }
    }
    done() {
        let loading = this.loading;
        window.setTimeout(function () {
            loading.addClass("visibility-hide");
        }, 1000);
    }
}
